// ingestionWorker/lib/cleanup.ts
import { prisma } from "./db";
import {
  GOSSIP_OBSERVATION_THRESHOLD,
  STATS_SAMPLE_THRESHOLD,
  INGESTION_RUN_THRESHOLD,
  CLEANUP_TRIGGER_PERCENT,
  CLEANUP_TARGET_PERCENT,
} from "../constants";

export async function checkAndCleanupIfNeeded() {
  // Count current rows
  const gossipCount = await prisma.pnodeGossipObservation.count();
  const statsCount = await prisma.pnodeStatsSample.count();
  const runsCount = await prisma.ingestionRun.count();

  // Check if any table exceeds 90% threshold
  const gossipThreshold =
    GOSSIP_OBSERVATION_THRESHOLD * CLEANUP_TRIGGER_PERCENT;
  const statsThreshold = STATS_SAMPLE_THRESHOLD * CLEANUP_TRIGGER_PERCENT;
  const runsThreshold = INGESTION_RUN_THRESHOLD * CLEANUP_TRIGGER_PERCENT;

  const needsCleanup =
    gossipCount > gossipThreshold ||
    statsCount > statsThreshold ||
    runsCount > runsThreshold;

  if (!needsCleanup) {
    return {
      cleanupNeeded: false,
      gossipCount,
      statsCount,
      runsCount,
    };
  }

  // Perform cleanup to bring down to 70% target
  const result = await performCleanup(gossipCount, statsCount, runsCount);

  return {
    cleanupNeeded: true,
    before: { gossipCount, statsCount, runsCount },
    after: result,
  };
}

async function performCleanup(
  currentGossip: number,
  currentStats: number,
  currentRuns: number
) {
  const results = {
    deletedGossip: 0,
    deletedStats: 0,
    deletedRuns: 0,
  };

  // Calculate target counts (70% of threshold)
  const gossipTarget = Math.floor(
    GOSSIP_OBSERVATION_THRESHOLD * CLEANUP_TARGET_PERCENT
  );
  const statsTarget = Math.floor(
    STATS_SAMPLE_THRESHOLD * CLEANUP_TARGET_PERCENT
  );
  const runsTarget = Math.floor(
    INGESTION_RUN_THRESHOLD * CLEANUP_TARGET_PERCENT
  );

  // Clean gossip observations if needed
  if (currentGossip > gossipTarget) {
    const toDelete = currentGossip - gossipTarget;
    const cutoffDate = await findNthOldestDate(
      "PnodeGossipObservation",
      "observedAt",
      toDelete
    );

    if (cutoffDate) {
      const deleted = await prisma.pnodeGossipObservation.deleteMany({
        where: { observedAt: { lt: cutoffDate } },
      });
      results.deletedGossip = deleted.count;
    }
  }

  // Clean stats samples if needed
  if (currentStats > statsTarget) {
    const toDelete = currentStats - statsTarget;
    const cutoffDate = await findNthOldestDate(
      "PnodeStatsSample",
      "timestamp",
      toDelete
    );

    if (cutoffDate) {
      const deleted = await prisma.pnodeStatsSample.deleteMany({
        where: { timestamp: { lt: cutoffDate } },
      });
      results.deletedStats = deleted.count;
    }
  }

  // Clean ingestion runs if needed
  if (currentRuns > runsTarget) {
    const toDelete = currentRuns - runsTarget;
    const cutoffDate = await findNthOldestDate(
      "IngestionRun",
      "startedAt",
      toDelete
    );

    if (cutoffDate) {
      const deleted = await prisma.ingestionRun.deleteMany({
        where: { startedAt: { lt: cutoffDate } },
      });
      results.deletedRuns = deleted.count;
    }
  }

  return results;
}

async function findNthOldestDate(
  table: string,
  dateField: string,
  n: number
): Promise<Date | null> {
  // Use raw query to find the nth oldest date efficiently
  const result = await prisma.$queryRaw<Array<{ cutoff: Date }>>`
    SELECT ${prisma.$queryRawUnsafe(dateField)} as cutoff
    FROM ${prisma.$queryRawUnsafe(`"${table}"`)}
    ORDER BY ${prisma.$queryRawUnsafe(dateField)} ASC
    LIMIT 1 OFFSET ${n}
  `;

  return result.length > 0 ? result[0].cutoff : null;
}
