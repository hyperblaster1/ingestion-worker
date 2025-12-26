// ingestionWorker/lib/prpc-client.ts
import * as http from "http";
import * as https from "https";

export type PrpcResponse<T> = {
  jsonrpc: string;
  result?: T;
  error?: { code: number; message: string; data?: unknown };
  id: number;
};

export type PodInfo = {
  address: string; // "ip:port"
  version?: string;
  last_seen?: string; // human-readable
  last_seen_timestamp?: number;
  pubkey?: string; // pNode public key (if available)
  // Storage fields from get-pods-with-stats
  storage_committed?: number;
  storage_used?: number;
  storage_usage_percent?: number; // Decimal (0-1) or percentage (0-100)?
  uptime?: number; // in seconds
  is_public?: boolean;
};

export type GetPodsWithStatsResult = {
  pods?: PodInfo[];
  total_count?: number;
};

export type GetPodsResult = GetPodsWithStatsResult | PodInfo[];

// Actual get-stats response structure
export type GetStatsResult = {
  active_streams?: number;
  cpu_percent?: number;
  current_index?: number;
  file_size?: number;
  last_updated?: number;
  packets_received?: number; // cumulative
  packets_sent?: number; // cumulative
  ram_total?: number;
  ram_used?: number;
  total_bytes?: number; // cumulative
  total_pages?: number;
  uptime?: number; // in seconds
};

async function prpcCall<T>(
  baseUrl: string,
  method: string,
  timeoutMs: number = 2500
): Promise<T> {
  // Parse the base URL and construct the full RPC endpoint URL
  const cleanBaseUrl = baseUrl.trim().replace(/\/$/, "");

  // Validate and parse the URL
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(cleanBaseUrl);
  } catch (e) {
    throw new Error(`Invalid base URL format: ${cleanBaseUrl}. Error: ${e}`);
  }

  // Ensure it's http or https
  if (!["http:", "https:"].includes(parsedUrl.protocol)) {
    throw new Error(
      `Unsupported protocol: ${parsedUrl.protocol}. Only http and https are supported.`
    );
  }

  // Construct the RPC endpoint URL
  const rpcUrl = new URL("/rpc", parsedUrl);

  // Use native http/https modules as undici has issues with some URLs
  const requestModule = rpcUrl.protocol === "https:" ? https : http;

  const requestBody = JSON.stringify({
    jsonrpc: "2.0",
    method,
    id: 1,
  });

  const json = await new Promise<PrpcResponse<T>>((resolve, reject) => {
    let timeoutId: NodeJS.Timeout | null = null;
    let isResolved = false;

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
    };

    const rejectOnce = (error: Error) => {
      if (!isResolved) {
        isResolved = true;
        cleanup();
        reject(error);
      }
    };

    const resolveOnce = (value: PrpcResponse<T>) => {
      if (!isResolved) {
        isResolved = true;
        cleanup();
        resolve(value);
      }
    };

    // Set timeout
    timeoutId = setTimeout(() => {
      req.destroy();
      rejectOnce(
        new Error(`pRPC request timeout after ${timeoutMs}ms for ${method}`)
      );
    }, timeoutMs);

    const req = requestModule.request(
      rpcUrl,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(requestBody),
        },
        timeout: timeoutMs,
      },
      (res) => {
        if (res.statusCode && (res.statusCode < 200 || res.statusCode >= 300)) {
          rejectOnce(
            new Error(`pRPC HTTP error ${res.statusCode} for ${method}`)
          );
          return;
        }

        let data = "";
        res.on("data", (chunk) => {
          data += chunk;
        });
        res.on("end", () => {
          try {
            const json = JSON.parse(data) as PrpcResponse<T>;
            resolveOnce(json);
          } catch (e) {
            rejectOnce(new Error(`Failed to parse JSON response: ${e}`));
          }
        });
        res.on("error", (err) => {
          res.destroy();
          req.destroy();
          rejectOnce(
            new Error(`pRPC response error for ${method}: ${err.message}`)
          );
        });
      }
    );

    req.on("error", (err) => {
      req.destroy();
      rejectOnce(
        new Error(`pRPC request failed for ${method}: ${err.message}`)
      );
    });

    req.on("timeout", () => {
      req.destroy();
      rejectOnce(new Error(`pRPC request timeout for ${method}`));
    });

    req.write(requestBody);
    req.end();
  });

  if (json.error) {
    throw new Error(
      `pRPC error for ${method}: ${json.error.code} ${json.error.message}`
    );
  }

  if (!json.result) {
    throw new Error(`pRPC: missing result for ${method}`);
  }

  return json.result;
}

export async function getPods(baseUrl: string): Promise<GetPodsResult> {
  // Use get-pods-with-stats to get storage data in one call
  return prpcCall<GetPodsResult>(baseUrl, "get-pods-with-stats");
}

export async function getStats(baseUrl: string): Promise<GetStatsResult> {
  return prpcCall<GetStatsResult>(baseUrl, "get-stats");
}

export type StorageCreditsResult = {
  pods_credits?: Array<{
    pod_id: string;
    credits: number;
  }>;
  status?: string;
};

/**
 * Fetch storage credits from the centralized API
 * Endpoint: https://podcredits.xandeum.network/api/pods-credits
 */
export async function getStorageCredits(): Promise<StorageCreditsResult> {
  const url = "https://podcredits.xandeum.network/api/pods-credits";

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
      // 10 second timeout for credits API
      signal: AbortSignal.timeout(10000),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data as StorageCreditsResult;
  } catch (error) {
    throw error;
  }
}
