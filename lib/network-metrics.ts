// ingestionWorker/lib/network-metrics.ts
import { prisma } from "./db";
import { DEFAULT_SEEDS } from "../config/seeds";

// Helper to calculate percentile
function percentile(sortedValues: number[], p: number): number {
  if (sortedValues.length === 0) return 0;
  const index = Math.ceil((p / 100) * sortedValues.length) - 1;
  return sortedValues[Math.max(0, Math.min(index, sortedValues.length - 1))];
}

export async function computeNetworkMetrics(ingestionRunId: number) {
  // Get all nodes with their latest data using pagination to avoid memory issues
  const BATCH_SIZE = 500;
  const nodes = [];
  let skip = 0;

  while (true) {
    const batch = await prisma.pnode.findMany({
      skip,
      take: BATCH_SIZE,
    select: {
      id: true,
      isPublic: true,
      failureCount: true,
      latestCredits: true,
      statsSamples: {
        orderBy: { timestamp: "desc" },
        take: 1,
        select: {
          uptimeSeconds: true,
        },
      },
      gossipObservations: {
        orderBy: { observedAt: "desc" },
        take: 1,
        select: {
          storageCommitted: true,
          storageUsed: true,
          seedBaseUrl: true,
          lastSeenTimestamp: true,
          observedAt: true,
          version: true,
        },
      },
    },
  });

    if (batch.length === 0) break;
    nodes.push(...batch);
    skip += BATCH_SIZE;
    
    // Safety limit to prevent infinite loop
    if (skip > 100000) break;
  }

  const now = Date.now() / 1000; // Current time in seconds (Unix timestamp)

  // A. Network Health metrics
  const totalNodes = nodes.length;
  const reachableNodes = nodes.filter((n) => n.isPublic).length;
  const unreachableNodes = totalNodes - reachableNodes;
  const reachablePercent = totalNodes > 0 ? (reachableNodes / totalNodes) * 100 : 0;

  // Get uptime values for median and p90
  const uptimes = nodes
    .map((n) => n.statsSamples[0]?.uptimeSeconds ?? null)
    .filter((u): u is number => u !== null && u > 0)
    .sort((a, b) => a - b);

  const medianUptimeSeconds = uptimes.length > 0 ? percentile(uptimes, 50) : 0;
  const p90UptimeSeconds = uptimes.length > 0 ? percentile(uptimes, 90) : 0;

  // Storage totals
  let totalStorageCommitted = BigInt(0);
  let totalStorageUsed = BigInt(0);

  for (const node of nodes) {
    const latestGossip = node.gossipObservations[0];
    if (latestGossip?.storageCommitted) {
      totalStorageCommitted += latestGossip.storageCommitted;
    }
    if (latestGossip?.storageUsed) {
      totalStorageUsed += latestGossip.storageUsed;
    }
  }

  // Reliability metrics
  const nodesBackedOff = nodes.filter(
    (n) => n.failureCount > 0
  ).length;
  const nodesFailingStats = nodes.filter(
    (n) => n.failureCount > 0 && !n.isPublic
  ).length;

  // B. Version Adoption
  const versionCounts = new Map<string, number>();
  for (const node of nodes) {
    const version = node.gossipObservations[0]?.version ?? "unknown";
    versionCounts.set(version, (versionCounts.get(version) ?? 0) + 1);
  }

  // C. Gossip Visibility (per seed)
  const seedVisibilityMap = new Map<
    string,
    {
      nodesSeen: number;
      freshNodes: number;
      staleNodes: number;
      offlineNodes: number;
    }
  >();

  // Initialize all seeds
  for (const seed of DEFAULT_SEEDS) {
    seedVisibilityMap.set(seed.baseUrl, {
      nodesSeen: 0,
      freshNodes: 0,
      staleNodes: 0,
      offlineNodes: 0,
    });
  }

  // Calculate visibility per seed - get latest observation per node per seed
  for (const seed of DEFAULT_SEEDS) {
    // Get all recent observations for this seed (last 10 minutes to ensure we capture active nodes)
    const recentObs = await prisma.pnodeGossipObservation.findMany({
      where: {
        seedBaseUrl: seed.baseUrl,
        observedAt: {
          gte: new Date(Date.now() - 10 * 60 * 1000),
        },
      },
      select: {
        pnodeId: true,
        lastSeenTimestamp: true,
        observedAt: true,
      },
      orderBy: {
        observedAt: "desc",
      },
    });

    // Get latest observation per node
    const latestPerNode = new Map<number, bigint | null>();
    for (const obs of recentObs) {
      if (!latestPerNode.has(obs.pnodeId)) {
        latestPerNode.set(obs.pnodeId, obs.lastSeenTimestamp);
      }
    }

    const visibility = seedVisibilityMap.get(seed.baseUrl)!;
    visibility.nodesSeen = latestPerNode.size;

    for (const [pnodeId, lastSeenTimestamp] of latestPerNode.entries()) {
      if (!lastSeenTimestamp) {
        visibility.offlineNodes++;
        continue;
      }

      // lastSeenTimestamp is in seconds (Unix timestamp)
      const lastSeenSeconds = Number(lastSeenTimestamp);
      const ageSeconds = now - lastSeenSeconds;

      if (ageSeconds < 30) {
        visibility.freshNodes++;
      } else if (ageSeconds < 120) {
        visibility.staleNodes++;
      } else {
        visibility.offlineNodes++;
      }
    }
  }

  // F. Credits distribution
  const credits = nodes
    .map((n) => n.latestCredits)
    .filter((c): c is number => c !== null && c !== undefined)
    .sort((a, b) => a - b);

  const medianCredits = credits.length > 0 ? percentile(credits, 50) : 0;
  const p90Credits = credits.length > 0 ? percentile(credits, 90) : 0;

  // Store everything in a transaction
  const networkSnapshot = await prisma.networkSnapshot.create({
    data: {
      ingestionRunId,
      totalNodes,
      reachableNodes,
      unreachableNodes,
      reachablePercent,
      medianUptimeSeconds,
      p90UptimeSeconds,
      totalStorageCommitted,
      totalStorageUsed,
      nodesBackedOff,
      nodesFailingStats,
      versionStats: {
        create: Array.from(versionCounts.entries()).map(([version, nodeCount]) => ({
          ingestionRunId,
          version,
          nodeCount,
        })),
      },
      seedVisibility: {
        create: Array.from(seedVisibilityMap.entries()).map(([seedBaseUrl, vis]) => ({
          ingestionRunId,
          seedBaseUrl,
          nodesSeen: vis.nodesSeen,
          freshNodes: vis.freshNodes,
          staleNodes: vis.staleNodes,
          offlineNodes: vis.offlineNodes,
        })),
      },
      creditsStat: {
        create: {
          ingestionRunId,
          medianCredits,
          p90Credits,
        },
      },
    },
    include: {
      versionStats: true,
      seedVisibility: true,
      creditsStat: true,
    },
  });

  return networkSnapshot;
}

