# Xandeum pNode Analytics Dashboard - Ingestion Worker

A background service that continuously monitors Xandeum pNodes through gossip-based observation, providing near-real-time network visibility and historical metrics.

---

## Overview

### What are Xandeum pNodes?

Xandeum pNodes (peer nodes) are distributed storage nodes in the Xandeum network. Each pNode stores data segments and participates in the network's gossip protocol to discover and communicate with other nodes. The health and behavior of these nodes—their reachability, storage usage, uptime, and version distribution—are essential signals for understanding network state.

### Why This Dashboard Exists

Observing pNodes is non-trivial for several reasons:

1. **No Central Registry**: There is no authoritative list of all pNodes. Nodes discover each other through gossip.
2. **Dynamic Availability**: Nodes come online and offline. Some are publicly reachable; others operate behind NAT or firewalls.
3. **Seed-Dependent Perspective**: Each "seed" node (a well-known entry point) maintains its own view of the network based on what it has learned through gossip. Different seeds may see different subsets of nodes.
4. **Time-Varying State**: A node's metrics (uptime, storage, packet rates) change over time. Historical tracking is necessary to understand trends.

This dashboard solves these problems by:

- Polling multiple seed nodes to aggregate a broader view of the network
- Tracking node state over time to build historical context
- Classifying nodes as reachable or private based on direct connectivity tests
- Providing per-seed and global network views for comparison

### Seed-Based Gossip Perspective

In the Xandeum network, **gossip** refers to the mechanism by which nodes share information about other nodes they know. When you query a seed node's `get-pods-with-stats` API, you receive that seed's current knowledge of the network—not necessarily the complete or real-time state of all nodes.

This means:

- Different seeds may report different node lists
- A node not visible to one seed may be visible to another
- Observation is probabilistic and time-lagged, not authoritative

This dashboard makes no claim to show every node or guarantee completeness. It shows an aggregated view based on what the configured seed nodes report.

---

## Architecture Overview

The system consists of three main components:

1. **Background Ingestion Worker** (this repository)

   - Node.js application running independently
   - Polls seed nodes via pRPC APIs
   - Writes observations and metrics to PostgreSQL
   - Runs continuously on a configurable interval (default: every 4 minutes)

2. **PostgreSQL Database**

   - Stores node identities, gossip observations, stats samples, and ingestion metadata
   - Managed by Railway in production
   - Implements automatic cleanup to prevent unbounded growth

3. **Web UI** (Next.js, separate repository)
   - Reads from PostgreSQL
   - Renders network views, node cards, time-series charts
   - Does not perform ingestion itself

### Data Flow

```
Seed pNodes (via pRPC APIs)
         ↓
   Ingestion Worker
    - get-pods-with-stats (gossip data)
    - get-stats (direct node metrics)
    - credits API (economic data)
         ↓
   PostgreSQL Database
         ↓
   Next.js API Routes
         ↓
   Web UI (React)
```

Ingestion and rendering are decoupled. The worker runs on its own schedule. The UI fetches the latest data from the database on demand.

---

## Data Sources & APIs Used

### 1. `get-pods-with-stats` (Gossip Observations)

**Endpoint**: `http://<seed-ip>:6000/rpc`  
**Method**: `get-pods-with-stats`

Returns a list of pNodes known to the seed, along with storage metadata. Each entry includes:

- `pubkey`: Unique identifier for the pNode
- `address`: IP:port where the node was last observed
- `version`: Software version reported by the node
- `last_seen_timestamp`: Unix timestamp of last gossip activity
- `storage_committed`: Bytes of storage the node has committed
- `storage_used`: Bytes currently in use
- `storage_usage_percent`: Usage as a percentage
- `is_public`: Whether the node is marked as publicly reachable

**Important**: This data represents the seed's gossip view, not a direct query to each node. The seed may report stale or incomplete information.

### 2. `get-stats` (Direct Node Metrics)

**Endpoint**: `http://<node-ip>:6000/rpc`  
**Method**: `get-stats`

Queries a specific pNode directly (if reachable) for real-time operational metrics:

- `uptime`: Seconds since the node started
- `packets_received` / `packets_sent`: Cumulative packet counts (used to compute per-second rates)
- `total_bytes`: Cumulative data transferred
- `active_streams`: Number of active data streams
- `ram_used` / `ram_total`: Memory usage
- `cpu_percent`: CPU utilization

**Important**: This query only succeeds if the node is publicly reachable. Nodes behind NAT or firewalls will fail. The worker implements exponential backoff for repeatedly failing nodes.

### 3. Storage Credits API

**Endpoint**: `https://podcredits.xandeum.network/api/pods-credits`

Returns economic credits associated with each pNode. Credits are an informational metric used by the network to track node contributions. The dashboard displays these values but makes no claims about their economic implications.

### Derived vs. Direct Data

- **Direct from Xandeum**: `pubkey`, `address`, `version`, `last_seen_timestamp`, `storage_*`, `uptime`, packet counts, credits
- **Derived by Worker**: `isPublic` (set to `true` if `get-stats` succeeds), `packetsInPerSec` / `packetsOutPerSec` (computed from cumulative deltas)
- **Not Inferred**: Geographic location, node operator identity, economic value

---

## Core Features

### 1. Seed-Based Network View

**What it shows**: A per-seed breakdown of which nodes each seed can observe, including fresh, stale, and offline classifications.

**Why it's useful**: Different seeds see different subsets of the network. Comparing seed perspectives helps identify partitions, connectivity issues, or nodes only visible to certain seeds.

**How it works**: For each configured seed, the worker calls `get-pods-with-stats` and records the results as `PnodeGossipObservation` entries, tagged with the seed's base URL. The UI can filter and group by seed.

### 2. Global Network View

**What it shows**: An aggregated count of unique nodes observed across all seeds, along with network-wide metrics like median uptime, total storage, and version distribution.

**Why it's useful**: Provides a single high-level snapshot of network health without requiring users to compare individual seed views.

**How it works**: The worker deduplicates nodes by `pubkey` across all seeds, computes aggregate statistics (median, p90), and stores the result in a `NetworkSnapshot` record after each ingestion cycle.

### 3. Node Cards (Summary Metrics)

**What it shows**: For each known node, a card displaying:

- Public key (truncated for readability)
- Reachability status (reachable / private)
- Latest software version
- Storage committed / used
- Latest observed address

**Why it's useful**: Quickly scan the fleet for outliers—nodes with unusual storage usage, outdated versions, or connectivity issues.

**How it works**: The UI queries the `Pnode` table and joins the latest `PnodeGossipObservation` and `PnodeStatsSample` to populate each card.

### 4. Reachable vs. Private Classification

**What it shows**: Whether a node is **reachable** (responds to direct `get-stats` queries) or **private** (visible in gossip but not directly queryable).

**Why it's useful**: Reachability affects data freshness. Unreachable nodes rely entirely on gossip data, which may be stale.

**How it works**: After recording gossip observations, the worker attempts `get-stats` on each node. If successful, the node's `isPublic` field is set to `true`. If it fails repeatedly, exponential backoff is applied (see below).

### 5. Version Distribution

**What it shows**: A breakdown of how many nodes are running each software version.

**Why it's useful**: Helps identify upgrade adoption rates and detect version fragmentation.

**How it works**: The worker groups nodes by the `version` field from gossip observations and stores counts in `NetworkVersionStat` records.

### 6. Storage Metrics

**What it shows**: For each node:

- `storage_committed`: Total storage allocated
- `storage_used`: Actual bytes in use
- `storage_usage_percent`: Percentage utilization

**Why it's useful**: Identifies nodes nearing capacity or under-utilized nodes.

**How it works**: These values come directly from `get-pods-with-stats`. The worker does not compute or infer storage capacity.

### 7. Credits Display

**What it shows**: The number of "credits" associated with each pNode, as reported by the Xandeum credits API.

**Why it's useful**: Credits are a network-internal metric that may correlate with node activity or contribution.

**What credits represent**: Credits are informational. The dashboard makes no claims about their economic value, convertibility, or relationship to node operator incentives. They are displayed as-is from the external API.

**How it works**: The worker polls the credits API every 2 hours and stores snapshots in `PodCreditsSnapshot`. The latest value is attached to each node's summary.

### 8. Ingestion Status & Progress Indicators

**What it shows**: Metadata about the most recent ingestion cycle:

- When it started and finished
- How many nodes were attempted, succeeded, failed, or in backoff
- Per-seed observation counts

**Why it's useful**: Provides transparency into the worker's health and data freshness. If the worker stops or stalls, the UI can surface this.

**How it works**: Each ingestion cycle creates an `IngestionRun` record with success/failure counts. The UI queries the latest record to display status.

### 9. Exponential Backoff for Unreachable Nodes

**What it shows**: Nodes that repeatedly fail `get-stats` queries are polled less frequently.

**Why it's useful**: Reduces wasted network traffic and database load for persistently unreachable nodes (e.g., nodes behind firewalls or offline).

**How it works**:

- Each node has a `failureCount` and `nextStatsAllowedAt` field.
- On failure, `failureCount` increments and `nextStatsAllowedAt` is set to `now + (60 * 2^failureCount)` seconds (capped at 5 failures).
- On success, `failureCount` resets to 0.
- The worker skips nodes where `nextStatsAllowedAt > now`.

### 10. Batching of Node Queries

**What it shows**: The worker processes nodes in batches of 50 to avoid overwhelming the database or network.

**Why it's useful**: Prevents connection pool exhaustion and spreads load over time.

**How it works**: When fetching stats for hundreds of nodes, the worker groups requests into batches of 50 and processes them sequentially (parallel within each batch). This balances throughput with resource constraints.

### 11. Automatic Data Cleanup

**What it shows**: Old gossip observations and stats samples are automatically deleted when storage thresholds are approached.

**Why it's useful**: Prevents unbounded database growth that could exhaust disk space or degrade performance.

**How it works**:

- Thresholds:
  - `PnodeGossipObservation`: 1,000,000 rows (trigger cleanup at 90%, target 70%)
  - `PnodeStatsSample`: 500,000 rows (trigger cleanup at 90%, target 70%)
  - `IngestionRun`: 10,000 runs (trigger cleanup at 90%, target 70%)
- The worker checks row counts every hour. If any table exceeds 90% of its threshold, it deletes the oldest records to bring usage down to 70%.
- Deletion is time-based (oldest first), preserving recent data for analysis.

---

## Time-Series & Detailed Node View

### Node Drawer Concept

The UI includes a "drawer" or modal that opens when clicking a node card. This view shows detailed historical metrics for a single node:

- Uptime over time
- Packet rates (in/out per second)
- Storage usage trends
- Observation history (which seeds saw it, when)

### Why Lazy-Load?

Loading detailed time-series data for all nodes at once would overwhelm the UI and database. Instead, the drawer fetches data on-demand only when a user requests it.

### What Historical Data is Shown?

The dashboard shows whatever data remains after automatic cleanup. Typically:

- **Gossip observations**: Last few days to weeks (depending on node count and ingestion frequency)
- **Stats samples**: Last few weeks to months (lower volume than gossip)

No guarantees are made about retention duration. Data is cleaned as needed to manage storage.

---

## Network Metrics View

### What This View Represents

The network metrics view aggregates data across all nodes to provide fleet-wide insights:

- **Total nodes observed**: Count of unique `pubkey` values
- **Reachable vs. private split**: Percentage breakdown
- **Median and p90 uptime**: Statistical distribution of node uptime
- **Total storage committed and used**: Network-wide capacity and utilization
- **Version distribution**: Which software versions are most common

### How It Differs from Per-Node View

Instead of showing individual node details, this view treats the network as a single entity. It answers questions like:

- Is the network growing or shrinking?
- Are most nodes reachable?
- What is the typical node uptime?
- Is there version fragmentation?

### Future Signals (Not Yet Implemented)

Potential future additions (marked clearly as aspirational):

- Geographic distribution (if IP geolocation is added)
- Churn rate (nodes joining/leaving per day)
- Anomaly detection (sudden drops in reachability or uptime)
- Correlation analysis (e.g., version vs. uptime)

These are not currently available.

---

## Ingestion Model

### Background Independence

The ingestion worker runs as a separate process, independent of the web UI. This means:

- Ingestion continues even if the UI is down or not being accessed
- The UI always reads from the database; it never triggers ingestion
- Ingestion performance does not affect UI responsiveness

### Ingestion Interval

The worker is configured to run a full ingestion cycle every **4 minutes** (240 seconds). This interval is a balance between:

- **Freshness**: Capturing node state changes reasonably quickly
- **Load**: Avoiding excessive API calls to seed nodes
- **Cost**: Reducing database write volume

This interval is not guaranteed and may vary in practice due to network latency, database load, or API timeouts.

### Backoff Behavior for Failing Nodes

When a node fails to respond to `get-stats`:

1. The worker increments its `failureCount`
2. It calculates a backoff delay: `60 * 2^failureCount` seconds (capped at 5 failures = 1920 seconds ~32 minutes)
3. The node is skipped in subsequent cycles until the backoff period expires
4. If the node later succeeds, `failureCount` resets and normal polling resumes

This prevents the worker from wasting time on persistently unreachable nodes while still giving them periodic opportunities to recover.

### Why Decoupled?

Decoupling ingestion from rendering has several benefits:

- **Scalability**: The UI can serve many users without affecting ingestion
- **Resilience**: If the UI crashes, data collection continues
- **Simplicity**: Each component has a single responsibility
- **Testing**: Ingestion logic can be tested independently

---

## Deployment & Operations

### Current Deployment

The ingestion worker is deployed on **Railway**, a platform-as-a-service provider. The deployment consists of:

1. **Worker Service**: A Node.js process running `worker.ts` continuously
2. **PostgreSQL Database**: A Railway-managed PostgreSQL instance

### Database Management

- **Storage Monitoring**: Database disk usage is tracked. The worker implements automatic cleanup to prevent exhaustion.
- **Connection Limits**: The worker uses a connection pool with a limit of 5 connections to avoid overwhelming the database (Railway's PostgreSQL has limited concurrent connection capacity).
- **Cleanup Strategy**: When table row counts exceed thresholds (see "Automatic Data Cleanup" above), the worker deletes old records to free space.

### No Performance Claims

This README makes no claims about:

- Query latency or throughput
- Cost per node or cost per observation
- Uptime or availability guarantees

The system is designed for correctness and operational simplicity, not high-throughput or low-latency analytics.

---

## Local Development

### Prerequisites

- Node.js 18+
- PostgreSQL (local or remote)
- npm or pnpm

### Setup

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd ingestion-worker
   ```

2. **Install dependencies**:

   ```bash
   npm install
   ```

3. **Configure environment variables**:
   Create a `.env` file in the project root:

   ```env
   DATABASE_URL="postgresql://user:password@localhost:5432/xandeum?connection_limit=5&pool_timeout=10"
   ```

   For Railway, use the provided `DATABASE_URL` from the Railway dashboard. Include connection pooling parameters to avoid "too many clients" errors.

4. **Run database migrations**:

   ```bash
   npx prisma generate
   npx prisma db push
   ```

5. **Start the worker**:

   ```bash
   npm run dev
   ```

   This runs the worker in development mode using `ts-node`.

### Configuration

- **Seed nodes**: Edit `config/seeds.ts` to add or remove seed nodes
- **Ingestion interval**: Modify `INGEST_INTERVAL_SECONDS` in `constants.ts`
- **Cleanup thresholds**: Adjust `GOSSIP_OBSERVATION_THRESHOLD`, `STATS_SAMPLE_THRESHOLD`, etc. in `constants.ts`

### Running Tests

No automated tests are currently included. Testing is manual: observe the worker logs and verify database records.

---

## Limitations & Design Tradeoffs

### 1. Gossip-Based Visibility is Not Authoritative

This dashboard shows what seed nodes report, not the complete network state. Some nodes may exist but never be observed if no seed has learned about them.

### 2. Nodes May Appear and Disappear

A node visible in one ingestion cycle may not appear in the next. This could mean:

- The node went offline
- The seed's gossip view changed
- Network partitions or latency affected observation

The dashboard does not distinguish between "node is offline" and "seed stopped reporting the node."

### 3. Metrics Depend on Node Availability

`get-stats` metrics (uptime, packets, etc.) are only available for publicly reachable nodes. Nodes behind NAT or firewalls will have stale or missing metrics.

### 4. Credits Are Informational Only

The dashboard displays credits as reported by the Xandeum API. It makes no claims about:

- What credits represent economically
- Whether credits can be redeemed or exchanged
- How credits are calculated

Credits are shown for transparency, not as investment advice.

### 5. Retention Is Not Guaranteed

Historical data is retained as long as database storage allows. Automatic cleanup will delete old records to prevent disk exhaustion. Users should not rely on indefinite retention.

### 6. No Real-Time Guarantees

The worker runs on a fixed interval (4 minutes by default). Data is **near-real-time**, not real-time. A node could change state between ingestion cycles without detection.

---

## Future Improvements

Potential enhancements, clearly marked as aspirational:

- **Geographic Distribution**: Add IP geolocation to visualize node locations
- **Longer Historical Views**: Implement archival storage or aggregation for long-term trends
- **Alerting**: Notify operators when network health degrades (e.g., sudden drop in reachable nodes)
- **Advanced Analytics**: Correlate version with uptime, detect anomalies, predict failures
- **API Rate Limiting**: Add backpressure mechanisms if seed APIs become rate-limited
- **Multi-Region Seeds**: Poll seeds in different geographic regions for broader coverage

These are not currently implemented and are listed only for context.

---

## License

[Specify license if applicable, e.g., MIT, Apache 2.0, proprietary]

---

## Attribution

This project uses Xandeum pNode APIs:

- `get-pods-with-stats` (gossip observations)
- `get-stats` (direct node metrics)
- Storage credits API (`https://podcredits.xandeum.network/api/pods-credits`)

All pNode data is sourced from the Xandeum network. This dashboard is a monitoring tool and does not modify or control network behavior.

---

## Contact

For questions or issues, please open a GitHub issue or contact the project maintainers.
