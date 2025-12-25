# Ingestion Worker

Standalone ingestion worker for the analytics platform. This worker runs independently of the Next.js application and handles:

- Stats ingestion (every 20 seconds)
- Credits ingestion (every 1 hour)

## Setup

1. Install dependencies:
```bash
npm install
```

2. Generate Prisma client:
```bash
npm run prisma:generate
```

3. Set up environment variables:
Create a `.env` file with:
```
DATABASE_URL="postgresql://user:password@localhost:5432/database"
```

4. Run the worker:
```bash
npm start
```

## Development

The worker uses its own TypeScript configuration and dependencies, completely independent of the Next.js application. It has its own `package.json` and manages its own dependencies.

## Structure

- `worker.ts` - Main entry point
- `lib/` - Core ingestion logic
  - `db.ts` - Prisma client
  - `ingest.ts` - Main ingestion cycle
  - `ingest/credits.ts` - Credits ingestion
  - `prpc-client.ts` - pRPC client for API calls
  - `seed-service.ts` - Seed management
  - `storage-analytics.ts` - Analytics utilities
- `config/` - Configuration files
  - `seeds.ts` - Default seed nodes
- `prisma/` - Prisma schema (shared with main app)

## Running from Main Project

From the `analytics-platform` directory, you can run:
```bash
npm run worker
```

This will automatically `cd` into the ingestionWorker directory and start it.

