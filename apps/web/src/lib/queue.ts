import { Queue } from "bullmq";
import {
  buildQueueAddOptions,
  queueNames,
  refreshCollectionJobSchema,
  refreshMediaJobSchema,
  reindexRangeJobSchema,
  refreshTokenJobSchema,
  type QueueName
} from "@nft-platform/queue";
import { getRedisClient } from "./redis";

const globalQueueRegistry = globalThis as typeof globalThis & {
  __nftPlatformQueues__?: Map<QueueName, Queue>;
};

type EnqueuedJobResult = {
  jobId: string;
  status: "queued" | "running" | "done" | "failed";
  attempts: number;
  lastError: string | null;
};

export async function enqueueRefreshTokenJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshTokenJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshToken, parsedPayload);
}

export async function enqueueRefreshCollectionJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshCollectionJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshCollection, parsedPayload);
}

export async function enqueueRefreshMediaJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = refreshMediaJobSchema.parse(payload);
  return enqueueJob(queueNames.refreshMedia, parsedPayload);
}

export async function enqueueReindexRangeJob(payload: unknown): Promise<EnqueuedJobResult> {
  const parsedPayload = reindexRangeJobSchema.parse(payload);
  return enqueueJob(queueNames.reindexRange, parsedPayload);
}

function getQueue(queueName: QueueName): Queue {
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

async function enqueueJob(queueName: QueueName, payload: unknown): Promise<EnqueuedJobResult> {
  const queueAddOptions = buildQueueAddOptions(queueName, payload);
  const jobId = queueAddOptions.jobId;
  const queue = getQueue(queueName);

  const existingJob = await queue.getJob(jobId);

  if (existingJob) {
    const state = await existingJob.getState();

    if (shouldReenqueueExistingJob(payload, state)) {
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

  await queue.add(queueName, payload, queueAddOptions);

  return {
    jobId,
    status: "queued",
    attempts: 0,
    lastError: null
  };
}

function shouldReenqueueExistingJob(payload: unknown, state: string): boolean {
  if (state === "failed") {
    return true;
  }

  if (state !== "completed" || !payload || typeof payload !== "object") {
    return false;
  }

  const candidate = payload as Record<string, unknown>;

  return candidate.forceMetadata === true ||
    candidate.forceOwnership === true ||
    candidate.forceDownload === true ||
    candidate.fullRescan === true;
}

function mapBullMqStateToJobStatus(state: string): "queued" | "running" | "done" | "failed" {
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