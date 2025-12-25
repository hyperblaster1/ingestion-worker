// ingestionWorker/worker.ts
// Standalone ingestion worker - runs independently of Next.js UI
import "dotenv/config";
import { prisma } from "./lib/db";
import { runIngestionCycle } from "./lib/ingest";
import { ingestCredits } from "./lib/ingest/credits";
import { computeNetworkMetrics } from "./lib/network-metrics";
import { INGEST_INTERVAL_SECONDS, CREDITS_INTERVAL_SECONDS } from "./constants";

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
      console.log(
        `[IngestionWorker] Network metrics computed for run ${run.id}`
      );
    } catch (err) {
      console.error(
        `[IngestionWorker] Failed to compute network metrics for run ${run.id}:`,
        err
      );
      // Don't fail the entire run if network metrics fail
    }

    const attemptsMsg =
      stats.statsAttempts > 0
        ? `${stats.statsSuccess}/${stats.statsAttempts} success`
        : `0 attempts (all in backoff)`;
    console.log(
      `[IngestionWorker] Run ${run.id} completed: ${attemptsMsg}, ${stats.backoffCount} backoff, ${stats.statsFailure} failed`
    );
  } catch (e) {
    console.error(`[IngestionWorker] Run ${run.id} failed:`, e);
    try {
      await prisma.ingestionRun.update({
        where: { id: run.id },
        data: { finishedAt: new Date() },
      });
    } catch (updateErr) {
      console.error(
        `[IngestionWorker] Failed to update run ${run.id} finish time:`,
        updateErr
      );
    }
    throw e;
  }
}

async function runCreditsOnce() {
  try {
    console.log("[IngestionWorker] Running credits ingestion");
    const result = await ingestCredits();
    console.log(
      `[IngestionWorker] Credits ingestion completed: ${result.totalProcessed} processed, ${result.totalSkipped} skipped, ${result.totalErrors} errors`
    );
  } catch (err) {
    console.error("[IngestionWorker] Credits ingestion failed:", err);
  }
}

async function main() {
  console.log("[IngestionWorker] Starting ingestion worker");
  console.log(
    `[IngestionWorker] Stats ingestion interval: ${INGEST_INTERVAL_SECONDS} seconds`
  );
  console.log(
    `[IngestionWorker] Credits ingestion interval: ${CREDITS_INTERVAL_SECONDS} seconds`
  );

  // Run stats ingestion once immediately on startup
  await runOnce().catch((err) => {
    console.error("[IngestionWorker] Initial stats ingestion run failed:", err);
  });

  // Run credits ingestion once immediately on startup
  await runCreditsOnce();

  // Stats ingestion: every INGEST_INTERVAL_SECONDS
  setInterval(() => {
    runOnce().catch((err) => {
      console.error(
        "[IngestionWorker] Scheduled stats ingestion run failed:",
        err
      );
    });
  }, INGEST_INTERVAL_SECONDS * 1000);

  // Credits ingestion: every CREDITS_INTERVAL_SECONDS
  setInterval(() => {
    runCreditsOnce();
  }, CREDITS_INTERVAL_SECONDS * 1000);
}

// Handle graceful shutdown
process.on("SIGINT", async () => {
  console.log("[IngestionWorker] Received SIGINT, shutting down gracefully");
  await prisma.$disconnect();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  console.log("[IngestionWorker] Received SIGTERM, shutting down gracefully");
  await prisma.$disconnect();
  process.exit(0);
});

main().catch((err) => {
  console.error("[IngestionWorker] Fatal error:", err);
  process.exit(1);
});
