// ingestionWorker/lib/cleanup.ts
import { prisma } from "./db";

/**
 * Cleanup old observation data to prevent unbounded database growth
 * Keeps the last 7 days of gossip observations and 30 days of stats samples
 */
export async function cleanupOldData() {
  const now = new Date();

  // Keep gossip observations for last 7 days
  const gossipCutoff = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);

  // Keep stats samples for last 30 days
  const statsCutoff = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  // Keep ingestion runs for last 30 days (cascades to network snapshots)
  const ingestionRunCutoff = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  try {
    // Delete old gossip observations
    const deletedGossip = await prisma.pnodeGossipObservation.deleteMany({
      where: {
        observedAt: {
          lt: gossipCutoff,
        },
      },
    });

    // Delete old stats samples
    const deletedStats = await prisma.pnodeStatsSample.deleteMany({
      where: {
        timestamp: {
          lt: statsCutoff,
        },
      },
    });

    // Delete old ingestion runs (and their associated data)
    const deletedRuns = await prisma.ingestionRun.deleteMany({
      where: {
        startedAt: {
          lt: ingestionRunCutoff,
        },
      },
    });

    return {
      deletedGossipObservations: deletedGossip.count,
      deletedStatsSamples: deletedStats.count,
      deletedIngestionRuns: deletedRuns.count,
    };
  } catch (err) {
    throw err;
  }
}
