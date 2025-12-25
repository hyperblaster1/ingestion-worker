// ingestionWorker/lib/storage-analytics.ts
import { PnodeStatsSample } from '@prisma/client';

// Safe BigInt → number conversion with cap
export function bigIntToNumberSafe(
  value: bigint | null | undefined,
): number | null {
  if (value == null) return null;
  const maxSafe = BigInt(Number.MAX_SAFE_INTEGER);
  if (value > maxSafe) return Number(maxSafe);
  if (value < BigInt(0)) return 0;
  return Number(value);
}

// Compute bytesPerSecond given previous + latest sample
export function computeBytesPerSecond(
  prev: PnodeStatsSample | null,
  latest: PnodeStatsSample,
): {
  deltaBytes: bigint | null;
  deltaSeconds: number | null;
  bytesPerSecond: number | null;
} {
  if (!latest.totalBytes || !latest.timestamp) {
    return { deltaBytes: null, deltaSeconds: null, bytesPerSecond: null };
  }

  if (!prev || !prev.totalBytes || !prev.timestamp) {
    return { deltaBytes: null, deltaSeconds: null, bytesPerSecond: null };
  }

  const deltaBytes = latest.totalBytes - prev.totalBytes;
  if (deltaBytes < BigInt(0)) {
    // Counter reset due to restart; treat as no rate
    return { deltaBytes: null, deltaSeconds: null, bytesPerSecond: null };
  }

  const deltaMs = latest.timestamp.getTime() - prev.timestamp.getTime();
  const deltaSeconds = Math.floor(deltaMs / 1000);

  if (deltaSeconds <= 5) {
    // Too small / noisy; ignore
    return { deltaBytes: deltaBytes, deltaSeconds, bytesPerSecond: null };
  }

  const deltaBytesNumber = bigIntToNumberSafe(deltaBytes);
  if (deltaBytesNumber == null) {
    return { deltaBytes, deltaSeconds, bytesPerSecond: null };
  }

  const bytesPerSecond = deltaBytesNumber / deltaSeconds;

  return {
    deltaBytes,
    deltaSeconds,
    bytesPerSecond,
  };
}

