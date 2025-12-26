// ingestionWorker/lib/ingest.ts
import { prisma } from "./db";
import { DEFAULT_SEEDS } from "../config/seeds";
import {
  getPods,
  getStats,
  type PodInfo,
  type GetPodsResult,
} from "./prpc-client";

function computeNextBackoff(failureCount: number, baseSeconds = 60): number {
  const cappedFailures = Math.min(failureCount, 5);
  return baseSeconds * Math.pow(2, cappedFailures);
}

export async function runIngestionCycle() {
  const seeds = DEFAULT_SEEDS;

  let totalPods = 0;
  let gossipObs = 0;
  let statsAttempts = 0;
  let statsSuccess = 0;
  let statsFailure = 0;

  const now = new Date();

  // Track backoff globally (deduplicate by pnodeId across all seeds)
  const backoffPnodeIds = new Set<number>();
  // Track globally unique observed pnodes across all seeds
  const globalObservedPnodeIds = new Set<number>();

  // Process all seeds in parallel
  const seedResults = await Promise.allSettled(
    seeds.map(async (seed) => {
      const baseUrl = seed.baseUrl;
      let podsResult: GetPodsResult;

      try {
        podsResult = await getPods(baseUrl);
      } catch (err) {
        return {
          pods: [],
          statsTasks: [],
          seedBaseUrl: seed.baseUrl,
          attempted: 0,
          backoff: 0,
          observed: 0,
        };
      }

      // Handle different response structures
      // get-pods-with-stats returns { pods: [], total_count: number }
      const pods: PodInfo[] = Array.isArray(podsResult)
        ? podsResult
        : "pods" in podsResult && Array.isArray(podsResult.pods)
        ? podsResult.pods
        : [];

      // Track per-seed metrics
      let seedAttempted = 0;
      let seedBackoff = 0;
      const seedObservedPnodeIds = new Set<number>();

      // Process all pods in parallel for database operations
      const podResults = await Promise.allSettled(
        pods
          .filter((pod) => pod.pubkey) // Skip pods without pubkey (should not happen, but defensive)
          .map(async (pod) => {
            const {
              address,
              version,
              last_seen_timestamp,
              pubkey,
              storage_committed,
              storage_used,
              storage_usage_percent,
              is_public,
            } = pod;

            try {
              // 1) Upsert Pnode by pubkey (unique and persistent identifier)
              // pubkey is guaranteed to exist due to filter above
              // Update isPublic from the latest gossip observation if available
              const pnode = await prisma.pnode.upsert({
                where: { pubkey: pubkey! },
                update: {
                  isPublic: is_public ?? false,
                },
                create: {
                  pubkey: pubkey!,
                  isPublic: is_public ?? false,
                },
              });

              // Track observed pnode for this seed
              seedObservedPnodeIds.add(pnode.id);
              // Track globally unique observed pnode
              globalObservedPnodeIds.add(pnode.id);

              // 2) Insert gossip observation with current address and storage data
              await prisma.pnodeGossipObservation.create({
                data: {
                  seedBaseUrl: seed.baseUrl,
                  pnodeId: pnode.id,
                  address,
                  version: version ?? null,
                  lastSeenTimestamp:
                    last_seen_timestamp != null
                      ? BigInt(last_seen_timestamp)
                      : null,
                  observedAt: now,
                  storageCommitted:
                    storage_committed != null
                      ? BigInt(storage_committed)
                      : null,
                  storageUsed:
                    storage_used != null ? BigInt(storage_used) : null,
                  storageUsagePercent:
                    storage_usage_percent != null
                      ? storage_usage_percent
                      : null,
                  isPublic: is_public ?? null,
                },
              });

              // 3) Check if we should call get-stats for this pnode
              const freshPnode = await prisma.pnode.findUnique({
                where: { id: pnode.id },
              });

              if (!freshPnode) {
                return null;
              }

              const { nextStatsAllowedAt, failureCount } = freshPnode;

              if (nextStatsAllowedAt && nextStatsAllowedAt > now) {
                // respect backoff - track globally (deduplicated by pnodeId)
                backoffPnodeIds.add(pnode.id);
                seedBackoff++;
                return null;
              }

              // This pnode will be attempted
              seedAttempted++;

              // Extract IP address and construct pod URL (always use port 6000)
              const ipAddress = address.split(":")[0];
              const statsBaseUrl = `http://${ipAddress}:6000`;

              return {
                pnodeId: pnode.id,
                seedBaseUrl: seed.baseUrl,
                address,
                statsBaseUrl,
                failureCount: failureCount ?? 0,
                baseUrl,
              };
            } catch (err) {
              throw err; // Re-throw to be caught by Promise.allSettled
            }
          })
      );

      const statsTasks: Array<{
        pnodeId: number;
        seedBaseUrl: string;
        address: string;
        statsBaseUrl: string;
        failureCount: number;
        baseUrl: string;
      }> = [];

      // Track failed pods
      for (const result of podResults) {
        if (result.status === "fulfilled" && result.value !== null) {
          statsTasks.push(result.value);
        }
      }

      return {
        pods,
        statsTasks,
        seedBaseUrl: seed.baseUrl,
        attempted: seedAttempted,
        backoff: seedBackoff,
        observed: seedObservedPnodeIds.size,
      };
    })
  );

  // Collect all stats tasks and process them in parallel
  const allStatsTasks: Array<{
    pnodeId: number;
    seedBaseUrl: string;
    address: string;
    statsBaseUrl: string;
    failureCount: number;
    baseUrl: string;
  }> = [];

  // Track per-seed stats from seed processing
  const seedStatsMap = new Map<
    string,
    {
      seedBaseUrl: string;
      attempted: number;
      backoff: number;
      observed: number;
      success: number;
      failed: number;
    }
  >();

  for (const seedResult of seedResults) {
    if (seedResult.status === "fulfilled") {
      totalPods += seedResult.value.pods.length;
      gossipObs += seedResult.value.pods.length;
      allStatsTasks.push(...seedResult.value.statsTasks);

      // Initialize per-seed stats
      seedStatsMap.set(seedResult.value.seedBaseUrl, {
        seedBaseUrl: seedResult.value.seedBaseUrl,
        attempted: seedResult.value.attempted,
        backoff: seedResult.value.backoff,
        observed: seedResult.value.observed,
        success: 0,
        failed: 0,
      });
    }
  }

  // DEDUPLICATE: Only keep first task per pnodeId to avoid redundant API calls
  // If 3 seeds all see the same node, we only need to fetch stats once
  const uniqueStatsTasks = new Map<number, (typeof allStatsTasks)[0]>();
  for (const task of allStatsTasks) {
    if (!uniqueStatsTasks.has(task.pnodeId)) {
      uniqueStatsTasks.set(task.pnodeId, task);
    }
  }
  const deduplicatedStatsTasks = Array.from(uniqueStatsTasks.values());

  statsAttempts = deduplicatedStatsTasks.length;

  // Process DEDUPLICATED stats requests in parallel
  const statsResults = await Promise.allSettled(
    deduplicatedStatsTasks.map(async (task) => {
      const {
        pnodeId,
        seedBaseUrl,
        address,
        statsBaseUrl,
        failureCount,
        baseUrl,
      } = task;

      try {
        const stats = await getStats(statsBaseUrl);

        // Extract fields from actual API response structure
        const latestTotalBytes =
          stats.total_bytes != null ? BigInt(stats.total_bytes) : null;
        const uptimeSeconds = stats.uptime != null ? stats.uptime : null;
        const activeStreams = stats.active_streams ?? null;
        const packetsReceivedCumulative =
          stats.packets_received != null
            ? BigInt(stats.packets_received)
            : null;
        const packetsSentCumulative =
          stats.packets_sent != null ? BigInt(stats.packets_sent) : null;

        // Get previous sample for this pnode (any seed or same seed; choose simplest)
        const prevSample = await prisma.pnodeStatsSample.findFirst({
          where: { pnodeId },
          orderBy: { timestamp: "desc" },
        });

        const latestSampleTimestamp = new Date();

        // Compute packets per second from cumulative values
        let packetsInPerSec: number | null = null;
        let packetsOutPerSec: number | null = null;

        if (
          prevSample &&
          prevSample.timestamp &&
          packetsReceivedCumulative != null &&
          packetsSentCumulative != null &&
          prevSample.packetsReceivedCumulative != null &&
          prevSample.packetsSentCumulative != null
        ) {
          const deltaMs =
            latestSampleTimestamp.getTime() - prevSample.timestamp.getTime();
          const deltaSeconds = Math.floor(deltaMs / 1000);

          if (deltaSeconds > 5) {
            const deltaPacketsReceived =
              packetsReceivedCumulative - prevSample.packetsReceivedCumulative;
            const deltaPacketsSent =
              packetsSentCumulative - prevSample.packetsSentCumulative;

            // Only compute if delta is positive (counter didn't reset)
            if (deltaPacketsReceived >= BigInt(0)) {
              packetsInPerSec = Number(deltaPacketsReceived) / deltaSeconds;
            }
            if (deltaPacketsSent >= BigInt(0)) {
              packetsOutPerSec = Number(deltaPacketsSent) / deltaSeconds;
            }
          }
        }

        // Record stats
        await prisma.pnodeStatsSample.create({
          data: {
            pnodeId,
            seedBaseUrl,
            timestamp: latestSampleTimestamp,
            uptimeSeconds,
            packetsInPerSec,
            packetsOutPerSec,
            packetsReceivedCumulative,
            packetsSentCumulative,
            totalBytes: latestTotalBytes,
            activeStreams,
          },
        });

        // Update backoff state on success
        // Note: isPublic should be set from gossip data, not stats success
        await prisma.pnode.update({
          where: { id: pnodeId },
          data: {
            failureCount: 0,
            lastStatsAttemptAt: now,
            lastStatsSuccessAt: now,
            nextStatsAllowedAt: new Date(now.getTime() + 60 * 1000),
          },
        });

        return { success: true, pnodeId, failureCount, seedBaseUrl };
      } catch (err) {
        const newFailureCount = failureCount + 1;
        const delaySeconds = computeNextBackoff(newFailureCount, 60);

        await prisma.pnode.update({
          where: { id: pnodeId },
          data: {
            failureCount: newFailureCount,
            lastStatsAttemptAt: now,
            nextStatsAllowedAt: new Date(now.getTime() + delaySeconds * 1000),
          },
        });

        return { success: false, pnodeId, failureCount, seedBaseUrl };
      }
    })
  );

  // Count successes and failures (global and per-seed)
  for (const result of statsResults) {
    if (result.status === "fulfilled") {
      const value = result.value;
      if (value.success) {
        statsSuccess++;
        // Track per-seed success
        const seedStats = seedStatsMap.get(value.seedBaseUrl);
        if (seedStats) {
          seedStats.success++;
        }
      } else {
        statsFailure++;
        // Track per-seed failure
        const seedStats = seedStatsMap.get(value.seedBaseUrl);
        if (seedStats) {
          seedStats.failed++;
        }
      }
    } else {
      statsFailure++;
      // For rejected promises, we don't know which seed, so we can't track per-seed
    }
  }

  return {
    ok: true,
    seedsCount: seeds.length,
    totalPods,
    gossipObs,
    statsAttempts,
    statsSuccess,
    statsFailure,
    backoffCount: backoffPnodeIds.size, // Global count of unique pnodes in backoff
    observed: globalObservedPnodeIds.size, // Global count of unique pnodes observed across all seeds
    seedStats: Array.from(seedStatsMap.values()),
  };
}
