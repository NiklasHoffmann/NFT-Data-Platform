import { createJob } from "@nft-platform/db";
import type { Db } from "mongodb";

type QueueBackedJobStatus = "queued" | "running" | "done" | "failed";

type QueueBackedJobResult = {
  jobId: string;
  status: QueueBackedJobStatus;
  attempts: number;
  lastError: string | null;
};

type QueueBackedJobType = "refresh-token" | "refresh-collection" | "refresh-media" | "reindex-range";

export async function persistQueueBackedJobRecord(params: {
  database: Db;
  queuedJob: QueueBackedJobResult;
  type: QueueBackedJobType;
  payload: Record<string, unknown>;
  now: Date;
}): Promise<{ jobId: string; statusCode: number }> {
  const persistedJobId = await createJob(params.database, {
    queueJobId: params.queuedJob.jobId,
    type: params.type,
    payload: params.payload,
    status: params.queuedJob.status,
    attempts: params.queuedJob.attempts,
    lastError: params.queuedJob.lastError,
    createdAt: params.now,
    updatedAt: params.now
  });

  return {
    jobId: persistedJobId.toHexString(),
    statusCode: resolveQueueBackedJobHttpStatus(params.queuedJob.status)
  };
}

export function mapQueueBackedDiscoveryStatus(status: QueueBackedJobStatus): "queued" | "failed" {
  return status === "failed" ? "failed" : "queued";
}

function resolveQueueBackedJobHttpStatus(status: QueueBackedJobStatus): number {
  if (status === "queued" || status === "running") {
    return 202;
  }

  return 200;
}