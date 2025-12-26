// ingestionWorker/worker.ts
// Standalone ingestion worker - runs independently of Next.js UI
import "dotenv/config";
import { prisma } from "./lib/db";
import { runIngestionCycle } from "./lib/ingest";
import { ingestCredits } from "./lib/ingest/credits";
import { computeNetworkMetrics } from "./lib/network-metrics";
import { checkAndCleanupIfNeeded } from "./lib/cleanup";
import {
  INGEST_INTERVAL_SECONDS,
  CREDITS_INTERVAL_SECONDS,
  CLEANUP_CHECK_INTERVAL_SECONDS,
} from "./constants";

async function runOnce() {
  const startedAt = new Date();

  const run = await prisma.ingestionRun.create({
    data: {
      startedAt,
      attempted: 0,
      success: 0,
      backoff: 0,
      failed: 0,
    },
  });

  try {
    // Always run global ingestion (all seeds) - no seedId parameter
    const stats = await runIngestionCycle();

    await prisma.ingestionRun.update({
      where: { id: run.id },
      data: {
        finishedAt: new Date(),
        attempted: stats.statsAttempts,
        success: stats.statsSuccess,
        backoff: Math.max(0, stats.backoffCount), // Ensure non-negative
        failed: stats.statsFailure,
        observed: stats.observed ?? 0, // Global unique pnodes observed across all seeds
      },
    });

    // Store per-seed stats
    if (stats.seedStats && stats.seedStats.length > 0) {
      await prisma.ingestionRunSeedStats.createMany({
        data: stats.seedStats.map((seedStat) => ({
          ingestionRunId: run.id,
          seedBaseUrl: seedStat.seedBaseUrl,
          attempted: seedStat.attempted,
          backoff: seedStat.backoff,
          success: seedStat.success,
          failed: seedStat.failed,
          observed: seedStat.observed,
        })),
      });
    }

    // Compute and store network metrics
    try {
      await computeNetworkMetrics(run.id);
    } catch (err) {
      // Don't fail the entire run if network metrics fail
    }
  } catch (e) {
    try {
      await prisma.ingestionRun.update({
        where: { id: run.id },
        data: { finishedAt: new Date() },
      });
    } catch (updateErr) {
      // Failed to update run finish time
    }
    throw e;
  }
}

async function runCreditsOnce() {
  try {
    await ingestCredits();
  } catch (err) {
    // Credits ingestion failed
  }
}

async function main() {
  // Run stats ingestion once immediately on startup
  await runOnce().catch(() => {});

  // Run credits ingestion once immediately on startup
  await runCreditsOnce();

  // Check and cleanup if needed on startup
  await checkAndCleanupIfNeeded().catch(() => {});

  // Stats ingestion: every INGEST_INTERVAL_SECONDS
  setInterval(() => {
    runOnce().catch(() => {});
  }, INGEST_INTERVAL_SECONDS * 1000);

  // Credits ingestion: every CREDITS_INTERVAL_SECONDS
  setInterval(() => {
    runCreditsOnce();
  }, CREDITS_INTERVAL_SECONDS * 1000);

  // Cleanup check: every hour (or configured interval)
  setInterval(() => {
    checkAndCleanupIfNeeded().catch(() => {});
  }, CLEANUP_CHECK_INTERVAL_SECONDS * 1000);
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  await prisma.$disconnect();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await prisma.$disconnect();
  process.exit(0);
});

main().catch(() => {
  process.exit(1);
});
