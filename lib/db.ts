// ingestionWorker/lib/db.ts
import { PrismaClient } from '@prisma/client';

// Single PrismaClient instance per process
// Worker creates its own instance with limited connection pool
export const prisma = new PrismaClient({
  log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
  datasources: {
    db: {
      url: process.env.DATABASE_URL,
    },
  },
});

// Limit connection pool for worker (it only needs a few connections)
// This leaves more connections available for the frontend/API
// Adjust the DATABASE_URL to include: ?connection_limit=5&pool_timeout=10

