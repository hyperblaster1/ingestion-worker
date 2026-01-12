// ingestionWorker/worker.ts
// Standalone ingestion worker - runs independently of Next.js UI
import "dotenv/config";
import * as http from "http";
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

// Track worker state for health checks
let lastSuccessfulIngestion: Date | null = null;
let lastIngestionAttempt: Date | null = null;
let ingestionFailureCount = 0;
let workerStartTime = new Date();

// Circuit breaker state
const CIRCUIT_BREAKER_THRESHOLD = 5; // Fail 5 times in a row before opening circuit
const CIRCUIT_BREAKER_RESET_TIME = 5 * 60 * 1000; // 5 minutes before retry
let circuitBreakerOpen = false;
let circuitBreakerOpenTime: Date | null = null;

async function runOnce() {
  const startedAt = new Date();
  lastIngestionAttempt = startedAt;
  console.log(`[${startedAt.toISOString()}] Starting ingestion cycle...`);

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
    console.log(`[${new Date().toISOString()}] Running ingestion cycle...`);
    const stats = await runIngestionCycle();
    console.log(`[${new Date().toISOString()}] Ingestion cycle completed: attempted=${stats.statsAttempts}, success=${stats.statsSuccess}, failed=${stats.statsFailure}, observed=${stats.observed}`);

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
      console.log(`[${new Date().toISOString()}] Network metrics computed`);
    } catch (err) {
      console.error(`[${new Date().toISOString()}] Network metrics failed:`, err);
      // Don't fail the entire run if network metrics fail
    }
    
    const duration = new Date().getTime() - startedAt.getTime();
    console.log(`[${new Date().toISOString()}] Ingestion cycle finished in ${duration}ms`);
    lastSuccessfulIngestion = new Date();
    ingestionFailureCount = 0;
  } catch (e) {
    ingestionFailureCount++;
    console.error(`[${new Date().toISOString()}] Ingestion cycle failed:`, e);
    try {
      await prisma.ingestionRun.update({
        where: { id: run.id },
        data: { finishedAt: new Date() },
      });
    } catch (updateErr) {
      console.error(`[${new Date().toISOString()}] Failed to update run finish time:`, updateErr);
    }
    throw e;
  }
}

async function runCreditsOnce() {
  console.log(`[${new Date().toISOString()}] Starting credits ingestion...`);
  try {
    const result = await ingestCredits();
    console.log(`[${new Date().toISOString()}] Credits ingestion completed: processed=${result.totalProcessed}, skipped=${result.totalSkipped}, errors=${result.totalErrors}`);
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Credits ingestion failed:`, err);
  }
}

async function main() {
  console.log(`[${new Date().toISOString()}] Ingestion worker starting...`);
  console.log(`[${new Date().toISOString()}] Config: stats=${INGEST_INTERVAL_SECONDS}s, credits=${CREDITS_INTERVAL_SECONDS}s, cleanup=${CLEANUP_CHECK_INTERVAL_SECONDS}s`);
  
  // Validate startup dependencies (fail fast if critical dependencies unavailable)
  try {
    await validateStartup();
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Startup validation failed:`, err);
    console.error(`[${new Date().toISOString()}] Worker will exit. Please fix the issues above and restart.`);
    process.exit(1);
  }
  
  // Run stats ingestion once immediately on startup
  await runOnce().catch((err) => {
    console.error(`[${new Date().toISOString()}] Initial ingestion failed:`, err);
    // Log full error details for debugging
    if (err instanceof Error) {
      console.error(`[${new Date().toISOString()}] Error stack:`, err.stack);
    }
  });

  // Run credits ingestion once immediately on startup
  await runCreditsOnce();

  // Check and cleanup if needed on startup (non-blocking with timeout)
  console.log(`[${new Date().toISOString()}] Checking cleanup status...`);
  runCleanupWithTimeout().catch((err) => {
    console.error(`[${new Date().toISOString()}] Cleanup check failed:`, err);
  });

  console.log(`[${new Date().toISOString()}] Worker initialized. Running scheduled tasks...`);

  // Start health check HTTP server
  startHealthCheckServer();

  // Start heartbeat monitoring
  startHeartbeatMonitoring();

  // Stats ingestion: every INGEST_INTERVAL_SECONDS with circuit breaker
  setInterval(() => {
    runOnceWithCircuitBreaker().catch((err) => {
      console.error(`[${new Date().toISOString()}] Scheduled ingestion failed:`, err);
    });
  }, INGEST_INTERVAL_SECONDS * 1000);

  // Credits ingestion: every CREDITS_INTERVAL_SECONDS
  setInterval(() => {
    runCreditsOnce();
  }, CREDITS_INTERVAL_SECONDS * 1000);

  // Cleanup check: every hour (or configured interval) - non-blocking with timeout
  setInterval(() => {
    runCleanupWithTimeout().catch((err) => {
      console.error(`[${new Date().toISOString()}] Scheduled cleanup check failed:`, err);
    });
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

/**
 * Run ingestion with circuit breaker protection
 * Prevents hammering the system if it's consistently failing
 */
/**
 * Run cleanup with timeout protection
 * Prevents cleanup from blocking ingestion if it hangs
 */
async function runCleanupWithTimeout() {
  const CLEANUP_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes max
  
  return Promise.race([
    checkAndCleanupIfNeeded(),
    new Promise((_, reject) => {
      setTimeout(() => {
        reject(new Error("Cleanup timeout after 5 minutes"));
      }, CLEANUP_TIMEOUT_MS);
    }),
  ]).catch((err) => {
    console.error(`[${new Date().toISOString()}] Cleanup failed or timed out:`, err);
    // Don't throw - cleanup failure shouldn't stop ingestion
  });
}

/**
 * Start heartbeat monitoring
 * Logs heartbeat every 10 minutes and alerts if no successful ingestion in 30 minutes
 */
function startHeartbeatMonitoring() {
  const HEARTBEAT_INTERVAL_MS = 10 * 60 * 1000; // 10 minutes
  const ALERT_THRESHOLD_MS = 30 * 60 * 1000; // 30 minutes
  
  setInterval(() => {
    const now = new Date();
    const timeSinceLastSuccess = lastSuccessfulIngestion
      ? now.getTime() - lastSuccessfulIngestion.getTime()
      : Infinity;
    
    console.log(`[${now.toISOString()}] Heartbeat - Uptime: ${Math.floor((now.getTime() - workerStartTime.getTime()) / 1000)}s, Last success: ${lastSuccessfulIngestion?.toISOString() || 'never'}, Failures: ${ingestionFailureCount}`);
    
    if (timeSinceLastSuccess > ALERT_THRESHOLD_MS) {
      console.error(`[${now.toISOString()}] ALERT: No successful ingestion in ${Math.floor(timeSinceLastSuccess / 60000)} minutes!`);
    }
  }, HEARTBEAT_INTERVAL_MS);
}

/**
 * Validate startup dependencies
 * Tests database connection and at least one seed node
 * Fails fast with clear error if critical dependencies are unavailable
 */
async function validateStartup(): Promise<void> {
  console.log(`[${new Date().toISOString()}] Validating startup dependencies...`);
  
  // Test database connection
  try {
    await prisma.$queryRaw`SELECT 1`;
    console.log(`[${new Date().toISOString()}] ✓ Database connection healthy`);
  } catch (err) {
    const error = err instanceof Error ? err.message : String(err);
    console.error(`[${new Date().toISOString()}] ✗ Database connection failed:`, error);
    throw new Error(`Startup validation failed: Database connection unhealthy - ${error}`);
  }
  
  // Test at least one seed node
  const { DEFAULT_SEEDS } = await import("./config/seeds");
  const { getPods } = await import("./lib/prpc-client");
  
  let atLeastOneSeedWorks = false;
  const seedErrors: string[] = [];
  
  for (const seed of DEFAULT_SEEDS.slice(0, 3)) { // Test first 3 seeds
    try {
      await Promise.race([
        getPods(seed.baseUrl),
        new Promise((_, reject) => {
          setTimeout(() => reject(new Error("Timeout")), 5000);
        }),
      ]);
      atLeastOneSeedWorks = true;
      console.log(`[${new Date().toISOString()}] ✓ Seed ${seed.baseUrl} is reachable`);
      break; // One working seed is enough
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      seedErrors.push(`${seed.baseUrl}: ${error}`);
      console.warn(`[${new Date().toISOString()}] ✗ Seed ${seed.baseUrl} failed:`, error);
    }
  }
  
  if (!atLeastOneSeedWorks) {
    console.error(`[${new Date().toISOString()}] ✗ All tested seeds failed:`, seedErrors);
    throw new Error(`Startup validation failed: No seed nodes reachable. Errors: ${seedErrors.join('; ')}`);
  }
  
  console.log(`[${new Date().toISOString()}] ✓ Startup validation complete`);
}

/**
 * Check database connection health
 * Reconnects if connection is lost
 */
async function checkDatabaseConnection(): Promise<boolean> {
  try {
    await prisma.$queryRaw`SELECT 1`;
    return true;
  } catch (err) {
    console.error(`[${new Date().toISOString()}] Database connection check failed:`, err);
    try {
      // Attempt to reconnect
      await prisma.$disconnect();
      await prisma.$connect();
      console.log(`[${new Date().toISOString()}] Database reconnected successfully`);
      return true;
    } catch (reconnectErr) {
      console.error(`[${new Date().toISOString()}] Database reconnection failed:`, reconnectErr);
      return false;
    }
  }
}

async function runOnceWithCircuitBreaker() {
  // Check database connection before attempting ingestion
  const dbHealthy = await checkDatabaseConnection();
  if (!dbHealthy) {
    throw new Error("Database connection unhealthy");
  }

  // Check if circuit breaker is open
  if (circuitBreakerOpen) {
    if (circuitBreakerOpenTime) {
      const timeSinceOpen = Date.now() - circuitBreakerOpenTime.getTime();
      if (timeSinceOpen < CIRCUIT_BREAKER_RESET_TIME) {
        const remainingSeconds = Math.ceil((CIRCUIT_BREAKER_RESET_TIME - timeSinceOpen) / 1000);
        console.log(`[${new Date().toISOString()}] Circuit breaker OPEN - skipping ingestion. Retry in ${remainingSeconds}s`);
        return;
      } else {
        // Reset circuit breaker - try again
        console.log(`[${new Date().toISOString()}] Circuit breaker RESET - attempting ingestion`);
        circuitBreakerOpen = false;
        circuitBreakerOpenTime = null;
        ingestionFailureCount = 0;
      }
    }
  }

  try {
    await runOnce();
    // Success - reset circuit breaker
    if (circuitBreakerOpen) {
      console.log(`[${new Date().toISOString()}] Circuit breaker CLOSED - ingestion succeeded`);
      circuitBreakerOpen = false;
      circuitBreakerOpenTime = null;
    }
    ingestionFailureCount = 0;
  } catch (err) {
    ingestionFailureCount++;
    
    // Open circuit breaker if threshold reached
    if (ingestionFailureCount >= CIRCUIT_BREAKER_THRESHOLD && !circuitBreakerOpen) {
      circuitBreakerOpen = true;
      circuitBreakerOpenTime = new Date();
      console.error(`[${new Date().toISOString()}] Circuit breaker OPENED after ${ingestionFailureCount} consecutive failures`);
    }
    
    throw err;
  }
}

/**
 * Start HTTP health check server
 * Responds with worker status and last ingestion time
 */
function startHealthCheckServer() {
  const port = process.env.HEALTH_CHECK_PORT ? parseInt(process.env.HEALTH_CHECK_PORT) : 3001;
  
  const server = http.createServer(async (req, res) => {
    if (req.url === "/health" || req.url === "/") {
      try {
        // Test database connection
        await prisma.$queryRaw`SELECT 1`;
        const dbHealthy = true;
        
        const health = {
          status: "ok",
          uptime: Math.floor((Date.now() - workerStartTime.getTime()) / 1000),
          lastSuccessfulIngestion: lastSuccessfulIngestion?.toISOString() || null,
          lastIngestionAttempt: lastIngestionAttempt?.toISOString() || null,
          ingestionFailureCount,
          database: dbHealthy ? "connected" : "disconnected",
          timestamp: new Date().toISOString(),
        };

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(health, null, 2));
      } catch (err) {
        const health = {
          status: "error",
          error: err instanceof Error ? err.message : String(err),
          timestamp: new Date().toISOString(),
        };
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify(health, null, 2));
      }
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  server.listen(port, () => {
    console.log(`[${new Date().toISOString()}] Health check server listening on port ${port}`);
  });

  server.on("error", (err) => {
    console.error(`[${new Date().toISOString()}] Health check server error:`, err);
  });
}

main().catch((err) => {
  console.error(`[Worker] Fatal error in main:`, err);
  process.exit(1);
});
