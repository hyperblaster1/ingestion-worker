// ingestionWorker/lib/db.ts
import { PrismaClient } from '@prisma/client';

// Single PrismaClient instance per process
// Worker creates its own instance
export const prisma = new PrismaClient({
  log: process.env.NODE_ENV === 'development' ? ['error', 'warn'] : ['error'],
});

