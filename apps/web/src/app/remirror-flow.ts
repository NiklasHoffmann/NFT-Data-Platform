import {
  countTokensPendingMediaMirror,
  createJob,
  listTokenIdentitiesPendingMediaMirror,
  type TokenMediaMirrorIdentity
} from "@nft-platform/db";
import { refreshMediaJobSchema } from "@nft-platform/queue";
import { getWebRuntimeConfig } from "../lib/env";
import { getWebMongoDatabase } from "../lib/mongodb";
import { enqueueRefreshMediaJob } from "../lib/queue";

/**
 * How many tokens one click queues. A request that queued everything would outlive
 * the proxy timeout on any sizable dataset and abort in an unknown state, so the work
 * is handed out in batches: the counter shows what is left and clicking again
 * continues. Jobs are idempotent per token, so a repeated batch costs nothing.
 */
export const mediaMirrorBatchSize = 100;

export type RemirrorOutcome = {
  status: "queued" | "complete" | "failed";
  message: string;
};

export async function handleRemirrorSubmission(): Promise<RemirrorOutcome> {
  const config = getWebRuntimeConfig();
  const database = getWebMongoDatabase();

  try {
    const pendingTokens = await listTokenIdentitiesPendingMediaMirror({
      database,
      currentPublicBaseUrl: config.mediaPublicBaseUrl,
      limit: mediaMirrorBatchSize
    });

    if (pendingTokens.length === 0) {
      return {
        status: "complete",
        message: "Every token with a media source is already mirrored into the current bucket."
      };
    }

    const queuedCount = await queueMediaRefreshJobs(database, pendingTokens);
    const remainingCount = await countTokensPendingMediaMirror({
      database,
      currentPublicBaseUrl: config.mediaPublicBaseUrl
    });

    // The counter still includes what was just queued, because it measures what is
    // mirrored, not what is scheduled. Saying "n still pending" right after queueing
    // would read as if nothing happened, so name the worker as the thing to wait for.
    const unmirroredAfterQueueing = Math.max(remainingCount - queuedCount, 0);

    return {
      status: "queued",
      message:
        unmirroredAfterQueueing > 0
          ? `Queued ${queuedCount} media refresh jobs. The counter above drops as the worker finishes them, then ${unmirroredAfterQueueing} tokens remain for the next batch.`
          : `Queued ${queuedCount} media refresh jobs — every pending token is now scheduled. The counter above drops as the worker finishes them.`
    };
  } catch {
    return {
      status: "failed",
      message: "Could not queue media refresh jobs. Check that MongoDB and Redis are reachable."
    };
  }
}

async function queueMediaRefreshJobs(
  database: ReturnType<typeof getWebMongoDatabase>,
  tokens: TokenMediaMirrorIdentity[]
): Promise<number> {
  const timestamp = new Date();
  let queuedCount = 0;

  for (const token of tokens) {
    // forceDownload is what makes an already completed job re-enqueue; without it a
    // token mirrored against the previous store would be treated as done.
    const payload = refreshMediaJobSchema.parse({
      chainId: token.chainId,
      contractAddress: token.contractAddress,
      tokenId: token.tokenId,
      forceDownload: true
    });
    const queuedJob = await enqueueRefreshMediaJob(payload);

    await createJob(database, {
      queueJobId: queuedJob.jobId,
      type: "refresh-media",
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

export function buildRemirrorRedirectUrl(outcome: RemirrorOutcome): string {
  const searchParams = new URLSearchParams({
    view: "operations",
    status: outcome.status === "failed" ? "failed" : "queued",
    message: outcome.message
  });

  return `/?${searchParams.toString()}`;
}
