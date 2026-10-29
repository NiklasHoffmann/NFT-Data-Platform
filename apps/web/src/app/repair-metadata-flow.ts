import {
  countTokensPendingMetadata,
  createJob,
  listTokenIdentitiesPendingMetadata,
  type TokenMediaMirrorIdentity
} from "@nft-platform/db";
import { refreshTokenJobSchema } from "@nft-platform/queue";
import { getWebMongoDatabase } from "../lib/mongodb";
import { enqueueRefreshTokenJob } from "../lib/queue";

/**
 * Smaller than the media batch on purpose: a metadata refresh reads the chain and
 * then fetches the metadata document, so each job costs RPC budget. A smaller chunk
 * keeps that burst controllable while still clearing a backlog in a few clicks.
 */
export const metadataRepairBatchSize = 50;

export type RepairMetadataOutcome = {
  status: "queued" | "complete" | "failed";
  message: string;
};

export async function handleRepairMetadataSubmission(): Promise<RepairMetadataOutcome> {
  const database = getWebMongoDatabase();

  try {
    const pendingTokens = await listTokenIdentitiesPendingMetadata({
      database,
      limit: metadataRepairBatchSize
    });

    if (pendingTokens.length === 0) {
      return {
        status: "complete",
        message: "Every token has metadata. Nothing to repair."
      };
    }

    const queuedCount = await queueMetadataRefreshJobs(database, pendingTokens);
    const remainingCount = await countTokensPendingMetadata({ database });
    const remainingAfterQueueing = Math.max(remainingCount - queuedCount, 0);

    return {
      status: "queued",
      message:
        remainingAfterQueueing > 0
          ? `Queued ${queuedCount} metadata refresh jobs. The counter above drops as the worker finishes them, then ${remainingAfterQueueing} tokens remain for the next batch.`
          : `Queued ${queuedCount} metadata refresh jobs — every pending token is now scheduled. The counter above drops as the worker finishes them.`
    };
  } catch {
    return {
      status: "failed",
      message: "Could not queue metadata refresh jobs. Check that MongoDB and Redis are reachable."
    };
  }
}

async function queueMetadataRefreshJobs(
  database: ReturnType<typeof getWebMongoDatabase>,
  tokens: TokenMediaMirrorIdentity[]
): Promise<number> {
  const timestamp = new Date();
  let queuedCount = 0;

  for (const token of tokens) {
    // forceMetadata is what re-runs a job that already completed or failed, and it
    // also makes the worker force the media download it queues afterwards -- which is
    // the whole point here, since these tokens have no media URL yet.
    const payload = refreshTokenJobSchema.parse({
      chainId: token.chainId,
      contractAddress: token.contractAddress,
      tokenId: token.tokenId,
      forceMetadata: true,
      forceOwnership: false
    });
    const queuedJob = await enqueueRefreshTokenJob(payload, { allowReenqueueCompleted: true });

    await createJob(database, {
      queueJobId: queuedJob.jobId,
      type: "refresh-token",
      payload,
      status: queuedJob.status,
      attempts: queuedJob.attempts,
      lastError: queuedJob.lastError,
      createdAt: timestamp,
      updatedAt: new Date()
    });

    queuedCount += 1;
  }

  return queuedCount;
}

export function buildRepairMetadataRedirectUrl(outcome: RepairMetadataOutcome): string {
  const searchParams = new URLSearchParams({
    view: "operations",
    status: outcome.status === "failed" ? "failed" : "queued",
    message: outcome.message
  });

  return `/?${searchParams.toString()}`;
}
