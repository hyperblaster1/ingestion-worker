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

