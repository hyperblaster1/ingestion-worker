// ===== INGESTION WORKER CONSTANTS =====

// Ingestion Intervals (seconds)
export const INGEST_INTERVAL_SECONDS = 240; // 4 minutes
export const CREDITS_INTERVAL_SECONDS = 7200; // 2 hours

// Backoff Configuration
export const BASE_BACKOFF_SECONDS = 60;
export const MAX_BACKOFF_FAILURES = 5;

// Query Limits
export const MAX_CREDIT_DELTAS_BATCH_SIZE = 500;

// Network Metrics
export const NETWORK_METRICS_FRESH_NODE_THRESHOLD_SECONDS = 30;
export const NETWORK_METRICS_STALE_NODE_THRESHOLD_SECONDS = 120;
export const NETWORK_METRICS_RECENT_OBSERVATION_WINDOW_MINUTES = 10;

// Database Cleanup Thresholds (row counts)
export const GOSSIP_OBSERVATION_THRESHOLD = 1_000_000; // 1M rows
export const STATS_SAMPLE_THRESHOLD = 500_000; // 500K rows
export const INGESTION_RUN_THRESHOLD = 10_000; // 10K runs

// Cleanup trigger at 90% of threshold
export const CLEANUP_TRIGGER_PERCENT = 0.9;

// Cleanup target: bring down to 70% of threshold
export const CLEANUP_TARGET_PERCENT = 0.7;

// Check interval (in seconds)
export const CLEANUP_CHECK_INTERVAL_SECONDS = 3600; // 1 hour
