#!/bin/bash
# Manual restart script for ingestion worker
# Use this if you need to restart the worker to free up memory

echo "Stopping ingestion worker..."
pkill -f "ts-node.*worker.ts" || true

# Wait for process to fully stop
sleep 2

echo "Starting ingestion worker..."
cd "$(dirname "$0")"
npm run dev


