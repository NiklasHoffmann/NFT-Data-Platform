import { createHash } from "node:crypto";
import { nftStandardSchema } from "@nft-platform/domain";
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
  contractAddress: z.string().min(1),
  tokenId: z.string().min(1),
  forceMetadata: z.boolean().default(false),
  forceOwnership: z.boolean().default(false)
});

export type RefreshTokenJob = z.infer<typeof refreshTokenJobSchema>;

export const refreshCollectionJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: z.string().min(1),
  tokenIdHint: z.string().min(1).optional(),
  standard: nftStandardSchema.optional(),
  fullRescan: z.boolean().default(false)
});

export type RefreshCollectionJob = z.infer<typeof refreshCollectionJobSchema>;

export const refreshMediaJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: z.string().min(1),
  tokenId: z.string().min(1),
  forceDownload: z.boolean().default(false)
});

export type RefreshMediaJob = z.infer<typeof refreshMediaJobSchema>;

export const reindexRangeJobSchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: z.string().min(1),
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
