// ingestionWorker/lib/ingest/credits.ts
import { prisma } from '../db';
import { getStorageCredits } from '../prpc-client';

/**
 * Ingest credits for all pods from the centralized API
 * Enforces 2-hour polling rule per pod
 * API: https://podcredits.xandeum.network/api/pods-credits
 */
export async function ingestCredits() {
  const now = new Date();
  const twoHoursAgo = new Date(now.getTime() - 2 * 60 * 60 * 1000);

  let totalProcessed = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  try {
    // Fetch credits from the centralized API
    const creditsData = await getStorageCredits();

    if (!creditsData.pods_credits || !Array.isArray(creditsData.pods_credits)) {
      console.warn('[ingestCredits] Invalid response from credits API', creditsData);
      return {
        totalProcessed: 0,
        totalSkipped: 0,
        totalErrors: 1,
      };
    }

    // Process each pod's credits
    for (const pod of creditsData.pods_credits) {
      try {
        const podPubkey = pod.pod_id;
        const credits = pod.credits;

        if (!podPubkey || typeof credits !== 'number') {
          totalErrors++;
          continue;
        }

        // Check if we have a recent snapshot (within 2 hours)
        const latestSnapshot = await prisma.podCreditsSnapshot.findFirst({
          where: { podPubkey },
          orderBy: { observedAt: 'desc' },
        });

        if (
          latestSnapshot &&
          latestSnapshot.observedAt > twoHoursAgo
        ) {
          // Skip - too recent
          totalSkipped++;
          continue;
        }

        // Insert new snapshot (seedBaseUrl is optional, can be null for centralized API)
        await prisma.podCreditsSnapshot.create({
          data: {
            podPubkey,
            credits,
            observedAt: now,
            seedBaseUrl: null, // Centralized API doesn't have seed association
          },
        });

        // Update Pnode with latest credits if it exists
        const pnode = await prisma.pnode.findUnique({
          where: { pubkey: podPubkey },
        });

        if (pnode) {
          await prisma.pnode.update({
            where: { id: pnode.id },
            data: {
              latestCredits: credits,
              creditsUpdatedAt: now,
            },
          });
        }

        totalProcessed++;
      } catch (podError) {
        console.error(
          `[ingestCredits] Error processing pod ${pod.pod_id}:`,
          podError
        );
        totalErrors++;
      }
    }
  } catch (error) {
    console.error('[ingestCredits] Error fetching credits:', error);
    totalErrors++;
  }

  return {
    totalProcessed,
    totalSkipped,
    totalErrors,
  };
}

