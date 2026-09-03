import { Queue } from "bullmq";
import {
  enqueueQueueJob,
  queueNames,
  refreshCollectionJobSchema,
  refreshMediaJobSchema,
  reindexRangeJobSchema,
  refreshTokenJobSchema,
  type EnqueuedJobResult,
  type QueueName
} from "@nft-platform/queue";
import { getRedisClient } from "./redis";

const globalQueueRegistry = globalThis as typeof globalThis & {
  __nftPlatformQueues__?: Map<QueueName, Queue>;
};

export async function enqueueRefreshTokenJob(
  payload: unknown,
  options?: { allowReenqueueCompleted?: boolean }
): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshTokenJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshToken, parsedPayload, options);
}

export async function enqueueRefreshCollectionJob(
  payload: unknown,
  options?: { allowReenqueueCompleted?: boolean }
): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshCollectionJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshCollection, parsedPayload, options);
}

export async function enqueueRefreshMediaJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshMediaJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshMedia, parsedPayload);
}

export async function enqueueReindexRangeJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = reindexRangeJobSchema.parse(payload);
  return enqueueJob(queueNames.reindexRange, parsedPayload);
}

export function getQueue(queueName: QueueName): Queue {
  const registry = (globalQueueRegistry.__nftPlatformQueues__ ??= new Map());
  const existingQueue = registry.get(queueName);

  if (existingQueue) {
    return existingQueue;
  }

  const queue = new Queue(queueName, {
    connection: getRedisClient()
  });

  registry.set(queueName, queue);
  return queue;
}

async function enqueueJob(
  queueName: QueueName,
  payload: unknown,
  options?: { allowReenqueueCompleted?: boolean }
): Promise<EnqueuedJobResult> {
  return enqueueQueueJob({
    queue: getQueue(queueName),
    queueName,
    payload,
    allowReenqueueCompleted: options?.allowReenqueueCompleted ?? false
  });
}
