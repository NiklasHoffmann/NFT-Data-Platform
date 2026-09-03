import { createHash } from "node:crypto";
import { evmTokenIdSchema, nftStandardSchema, normalizedEvmAddressSchema } from "@nft-platform/domain";
import { z } from "zod";

export const queueNames = {
  refreshToken: "refresh-token",
  refreshCollection: "refresh-collection",
  refreshMedia: "refresh-media",
  reindexRange: "reindex-range"
} as const;

export type QueueName = (typeof queueNames)[keyof typeof queueNames];

export const refreshTokenJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: normalizedEvmAddressSchema,
  tokenId: evmTokenIdSchema,
  forceMetadata: z.boolean().default(false),
  forceOwnership: z.boolean().default(false)
});

export type RefreshTokenJob = z.infer<typeof refreshTokenJobSchema>;

export const refreshCollectionJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: normalizedEvmAddressSchema,
  tokenIdHint: evmTokenIdSchema.optional(),
  standard: nftStandardSchema.optional(),
  fullRescan: z.boolean().default(false)
});

export type RefreshCollectionJob = z.infer<typeof refreshCollectionJobSchema>;

export const refreshMediaJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: normalizedEvmAddressSchema,
  tokenId: evmTokenIdSchema,
  forceDownload: z.boolean().default(false)
});

export type RefreshMediaJob = z.infer<typeof refreshMediaJobSchema>;

export const reindexRangeJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: normalizedEvmAddressSchema,
  fromBlock: z.number().int().nonnegative(),
  toBlock: z.number().int().nonnegative()
});

export type ReindexRangeJob = z.infer<typeof reindexRangeJobSchema>;

export const mediaRefreshRetryPolicy = {
  attempts: 6,
  backoffDelayMs: 30_000
} as const;

export const jobPayloadSchemas = {
  [queueNames.refreshToken]: refreshTokenJobSchema,
  [queueNames.refreshCollection]: refreshCollectionJobSchema,
  [queueNames.refreshMedia]: refreshMediaJobSchema,
  [queueNames.reindexRange]: reindexRangeJobSchema
} as const;

export function buildIdempotencyKey(queueName: QueueName, payload: unknown): string {
  const digest = createHash("sha256").update(JSON.stringify(payload)).digest("hex");
  return `${queueName}-${digest}`;
}

export type QueueBackedJobStatus = "queued" | "running" | "done" | "failed";

export type EnqueuedJobResult = {
  jobId: string;
  status: QueueBackedJobStatus;
  attempts: number;
  lastError: string | null;
};

/**
 * The subset of a BullMQ queue this module needs. Declaring it structurally keeps the queue
 * package free of a runtime dependency on BullMQ while still being satisfied by a real `Queue`.
 */
type QueueLike = {
  getJob(jobId: string): Promise<QueueJobLike | undefined>;
  add(name: string, payload: unknown, options: ReturnType<typeof buildQueueAddOptions>): Promise<unknown>;
};

type QueueJobLike = {
  attemptsMade: number;
  getState(): Promise<string>;
  remove(): Promise<unknown>;
};

export function mapBullMqStateToJobStatus(state: string): QueueBackedJobStatus {
  switch (state) {
    case "completed":
      return "done";
    case "failed":
      return "failed";
    case "active":
      return "running";
    default:
      return "queued";
  }
}

/**
 * Adds a job under its idempotency key, so that concurrent producers converge on one job per
 * payload instead of queueing duplicates.
 *
 * A job id that is still present in BullMQ's completed set makes `add` a silent no-op, which is
 * the desired behaviour for an unchanged read but wrong for work that is meant to run again.
 * `allowReenqueueCompleted` covers that second case: the settled job is dropped first so the same
 * payload can be queued once more.
 */
export async function enqueueQueueJob(params: {
  queue: QueueLike;
  queueName: QueueName;
  payload: unknown;
  allowReenqueueCompleted?: boolean;
}): Promise<EnqueuedJobResult> {
  const queueAddOptions = buildQueueAddOptions(params.queueName, params.payload);
  const jobId = queueAddOptions.jobId;
  const existingJob = await params.queue.getJob(jobId);

  if (existingJob) {
    const state = await existingJob.getState();

    if (shouldReenqueueExistingJob(params.payload, state, params.allowReenqueueCompleted ?? false)) {
      await existingJob.remove();
    } else {
      return {
        jobId,
        status: mapBullMqStateToJobStatus(state),
        attempts: existingJob.attemptsMade,
        lastError: null
      };
    }
  }

  await params.queue.add(params.queueName, params.payload, queueAddOptions);

  return {
    jobId,
    status: "queued",
    attempts: 0,
    lastError: null
  };
}

function shouldReenqueueExistingJob(
  payload: unknown,
  state: string,
  allowReenqueueCompleted: boolean
): boolean {
  if (state === "failed") {
    return true;
  }

  if (state !== "completed") {
    return false;
  }

  if (allowReenqueueCompleted) {
    return true;
  }

  if (!payload || typeof payload !== "object") {
    return false;
  }

  const candidate = payload as Record<string, unknown>;

  return candidate.forceMetadata === true ||
    candidate.forceOwnership === true ||
    candidate.forceDownload === true ||
    candidate.fullRescan === true;
}

export function buildQueueAddOptions(queueName: QueueName, payload: unknown): {
  jobId: string;
  removeOnComplete: number;
  removeOnFail: number;
  attempts?: number;
  backoff?: {
    type: "exponential";
    delay: number;
  };
} {
  const options: {
    jobId: string;
    removeOnComplete: number;
    removeOnFail: number;
    attempts?: number;
    backoff?: {
      type: "exponential";
      delay: number;
    };
  } = {
    jobId: buildIdempotencyKey(queueName, payload),
    removeOnComplete: 500,
    removeOnFail: 500
  };

  if (queueName === queueNames.refreshMedia) {
    options.attempts = mediaRefreshRetryPolicy.attempts;
    options.backoff = {
      type: "exponential",
      delay: mediaRefreshRetryPolicy.backoffDelayMs
    };
  }

  return options;
}
