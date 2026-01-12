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

/**
 * Reset nodes that have been in backoff for too long (e.g., after worker downtime)
 * This ensures nodes aren't permanently stuck after the worker restarts
 */
async function resetStaleBackoffStates() {
  const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
  
  const result = await prisma.pnode.updateMany({
    where: {
      nextStatsAllowedAt: {
        lt: oneDayAgo, // If backoff expired more than a day ago
      },
      failureCount: {
        gt: 0, // And has a failure count
      },
    },
    data: {
      failureCount: 0,
      nextStatsAllowedAt: null,
    },
  });
  
  if (result.count > 0) {
    console.log(`[Ingest] Reset ${result.count} nodes from stale backoff state`);
  }
  
  return result.count;
}

export async function runIngestionCycle() {
  const seeds = DEFAULT_SEEDS;
  console.log(`[Ingest] Starting cycle with ${seeds.length} seeds`);

  // Reset nodes that have been in backoff too long (after downtime)
  await resetStaleBackoffStates();

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
        console.log(`[Ingest] Fetching pods from ${baseUrl}...`);
        podsResult = await getPods(baseUrl);
        const podCount = Array.isArray(podsResult) ? podsResult.length : podsResult.pods?.length || 0;
        console.log(`[Ingest] Received pods from ${baseUrl}: ${podCount} pods`);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        const errorStack = err instanceof Error ? err.stack : undefined;
        console.error(`[Ingest] Failed to fetch pods from ${baseUrl}:`, {
          error: errorMessage,
          stack: errorStack,
          seed: baseUrl,
          timestamp: new Date().toISOString(),
        });
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
              } else if (nextStatsAllowedAt && nextStatsAllowedAt <= now && failureCount > 0) {
                // Backoff expired but failureCount still set - should have been reset but clear it now
                await prisma.pnode.update({
                  where: { id: pnode.id },
                  data: { failureCount: 0, nextStatsAllowedAt: null },
                });
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
  console.log(`[Ingest] Total unique nodes to fetch stats: ${statsAttempts}`);
  console.log(`[Ingest] Backoff nodes (skipped): ${backoffPnodeIds.size}`);

  // Process DEDUPLICATED stats requests in batches of 50
  const BATCH_SIZE = 50;
  const statsResults: PromiseSettledResult<{
    success: boolean;
    pnodeId: number;
    failureCount: number;
    seedBaseUrl: string;
  }>[] = [];

  const totalBatches = Math.ceil(deduplicatedStatsTasks.length / BATCH_SIZE);
  console.log(`[Ingest] Processing ${totalBatches} batches of ${BATCH_SIZE}...`);

  for (let i = 0; i < deduplicatedStatsTasks.length; i += BATCH_SIZE) {
    const batch = deduplicatedStatsTasks.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    console.log(`[Ingest] Batch ${batchNum}/${totalBatches}: Processing ${batch.length} nodes...`);

    const batchResults = await Promise.allSettled(
      batch.map(async (task) => {
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
                packetsReceivedCumulative -
                prevSample.packetsReceivedCumulative;
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
        const errorMessage = err instanceof Error ? err.message : String(err);
        const newFailureCount = failureCount + 1;
        const delaySeconds = computeNextBackoff(newFailureCount, 60);

        // Log structured error for debugging
        console.error(`[Ingest] Stats fetch failed for pnode ${pnodeId} (${statsBaseUrl}):`, {
          error: errorMessage,
          pnodeId,
          statsBaseUrl,
          failureCount: newFailureCount,
          backoffSeconds: delaySeconds,
          seedBaseUrl,
          timestamp: new Date().toISOString(),
        });

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
    statsResults.push(...batchResults);
  }

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
      const errorMessage = result.reason instanceof Error ? result.reason.message : String(result.reason);
      const errorStack = result.reason instanceof Error ? result.reason.stack : undefined;
      console.error(`[Ingest] Promise rejected in stats batch:`, {
        error: errorMessage,
        stack: errorStack,
        timestamp: new Date().toISOString(),
      });
      // For rejected promises, we don't know which seed, so we can't track per-seed
    }
  }

  const summary = {
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

  console.log(`[Ingest] Cycle complete: pods=${summary.totalPods}, observed=${summary.observed}, attempted=${summary.statsAttempts}, success=${summary.statsSuccess}, failed=${summary.statsFailure}, backoff=${summary.backoffCount}`);

  return summary;
}
