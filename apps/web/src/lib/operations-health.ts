import { Queue } from "bullmq";
import type { Db } from "mongodb";
import { queueNames, type QueueName } from "@nft-platform/queue";
import {
  countTokensPendingMediaMirror,
  countTokensPendingMetadata,
  countTokensWithDeadMetadataSource,
  countTokensWithoutMetadataUri,
  listCollectionsWithIndexingLag,
  listMetadataErrorGroups,
  type MetadataErrorGroup,
  type IndexingLagCollection,
  getMongoCollections,
  listWorkerHeartbeats,
  serializeCollectionDocument,
  serializeJobDocument,
  type CollectionDocument,
  type JobDocument,
  type WorkerHeartbeatDocument
} from "@nft-platform/db";
import { getRedisClient } from "./redis";

type SerializedJob = ReturnType<typeof serializeJobDocument>;
const queueBackedJobTypes = new Set<QueueName>(Object.values(queueNames));
const globalQueueRegistry = globalThis as typeof globalThis & {
  __nftPlatformOperationsQueues__?: Map<QueueName, Queue>;
};

/**
 * A worker that has not reported within this window is treated as gone. The worker writes every
 * 30 seconds, so this tolerates two missed writes before the view calls it stale.
 */
const workerStaleAfterSeconds = 120;

export type WorkerHealth = {
  instanceId: string;
  startedAt: string;
  lastSeenAt: string;
  ageSeconds: number;
  isStale: boolean;
  chainIndexingEnabled: boolean;
  chainIndexingPollIntervalMs: number;
  chainIndexingAllowlistSize: number;
  metadataSweepEnabled: boolean;
  metadataSweepIntervalMs: number;
};

export type OperationsHealthSnapshot = {
  summary: {
    activeJobsCount: number;
    retryingMediaJobsCount: number;
    failedJobsCount: number;
    laggingCollectionsCount: number;
    pendingMediaMirrorCount: number;
    pendingMetadataCount: number;
    tokensWithoutMetadataUriCount: number;
    deadMetadataSourceCount: number;
    liveWorkerCount: number;
    chainIndexingEnabled: boolean;
    metadataSweepEnabled: boolean;
  };
  workers: WorkerHealth[];
  activeJobs: SerializedJob[];
  retryingMediaJobs: SerializedJob[];
  recentFailedJobs: SerializedJob[];
  indexingLagCollections: IndexingLagCollection[];
  metadataErrorGroups: MetadataErrorGroup[];
  laggingCollections: Array<
    ReturnType<typeof serializeCollectionDocument> & {
      lagBlocks: number;
      indexedCheckpoint: number;
      observedCheckpoint: number;
    }
  >;
};

export async function loadOperationsHealth(
  database: Db,
  params: { currentMediaPublicBaseUrl: string }
): Promise<OperationsHealthSnapshot> {
  const collections = getMongoCollections(database);
  const liveActiveJobs = await listLiveQueueJobs(database);
  const retryingMediaJobs = liveActiveJobs.filter((job) => job.type === "refresh-media" && job.attempts > 1);
  const now = new Date();

  const [
    recentFailedJobs,
    laggingCollections,
    failedJobsCount,
    laggingCollectionsCount,
    pendingMediaMirrorCount,
    pendingMetadataCount,
    indexingLagCollections,
    tokensWithoutMetadataUriCount,
    metadataErrorGroups,
    deadMetadataSourceCount,
    workerHeartbeats
  ] = await Promise.all([
    collections.jobs
      .find({ status: "failed" })
      .sort({ updatedAt: -1, _id: -1 })
      .limit(6)
      .toArray(),
    listLaggingCollections(database, 6),
    collections.jobs.countDocuments({ status: "failed" }),
    countLaggingCollections(database),
    countTokensPendingMediaMirror({
      database,
      currentPublicBaseUrl: params.currentMediaPublicBaseUrl
    }),
    countTokensPendingMetadata({ database }),
    listCollectionsWithIndexingLag({ database, limit: 6 }),
    countTokensWithoutMetadataUri({ database }),
    listMetadataErrorGroups({ database, limit: 5 }),
    countTokensWithDeadMetadataSource({ database }),
    listWorkerHeartbeats({ database })
  ]);

  const workers = workerHeartbeats.map((heartbeat) => buildWorkerHealth(heartbeat, now));
  const liveWorkers = workers.filter((worker) => !worker.isStale);

  return {
    summary: {
      activeJobsCount: liveActiveJobs.length,
      retryingMediaJobsCount: retryingMediaJobs.length,
      failedJobsCount,
      laggingCollectionsCount,
      pendingMediaMirrorCount,
      pendingMetadataCount,
      tokensWithoutMetadataUriCount,
      deadMetadataSourceCount,
      liveWorkerCount: liveWorkers.length,
      // Reported as "on" only while a worker that has it enabled is actually reporting. A switched-on
      // flag on a worker that died says nothing about whether anything is being indexed right now.
      chainIndexingEnabled: liveWorkers.some((worker) => worker.chainIndexingEnabled),
      metadataSweepEnabled: liveWorkers.some((worker) => worker.metadataSweepEnabled)
    },
    workers,
    activeJobs: liveActiveJobs.slice(0, 8).map(serializeJobDocument),
    retryingMediaJobs: retryingMediaJobs.slice(0, 6).map(serializeJobDocument),
    recentFailedJobs: recentFailedJobs.map(serializeJobDocument),
    indexingLagCollections,
    metadataErrorGroups,
    laggingCollections
  };
}

function buildWorkerHealth(heartbeat: WorkerHeartbeatDocument, now: Date): WorkerHealth {
  const ageSeconds = Math.max(0, Math.floor((now.getTime() - heartbeat.lastSeenAt.getTime()) / 1000));

  return {
    instanceId: heartbeat.instanceId,
    startedAt: heartbeat.startedAt.toISOString(),
    lastSeenAt: heartbeat.lastSeenAt.toISOString(),
    ageSeconds,
    isStale: ageSeconds > workerStaleAfterSeconds,
    chainIndexingEnabled: heartbeat.chainIndexingEnabled,
    chainIndexingPollIntervalMs: heartbeat.chainIndexingPollIntervalMs,
    chainIndexingAllowlistSize: heartbeat.chainIndexingAllowlistSize,
    metadataSweepEnabled: heartbeat.metadataSweepEnabled,
    metadataSweepIntervalMs: heartbeat.metadataSweepIntervalMs
  };
}

async function listLiveQueueJobs(database: Db): Promise<JobDocument[]> {
  const queuedOrRunningJobs = await getMongoCollections(database)
    .jobs.find({ status: { $in: ["queued", "running"] } })
    .sort({ updatedAt: -1, _id: -1 })
    .toArray();

  if (queuedOrRunningJobs.length === 0) {
    return [];
  }

  const resolvedJobs = await Promise.all(
    queuedOrRunningJobs.map(async (job) => {
      const liveStatus = await resolveLiveQueueJobStatus(job);

      if (liveStatus !== "queued" && liveStatus !== "running") {
        return null;
      }

      return {
        ...job,
        status: liveStatus
      } satisfies JobDocument;
    })
  );

  return resolvedJobs.reduce<JobDocument[]>((liveJobs, job) => {
    if (job) {
      liveJobs.push(job);
    }

    return liveJobs;
  }, []);
}

async function resolveLiveQueueJobStatus(job: JobDocument): Promise<JobDocument["status"] | "missing"> {
  if (!job.queueJobId || !isQueueBackedJobType(job.type)) {
    return job.status;
  }

  const queue = getOperationsQueue(job.type);
  const queueJob = await queue.getJob(job.queueJobId);

  if (!queueJob) {
    return "missing";
  }

  return mapBullMqStateToJobStatus(await queueJob.getState());
}

function getOperationsQueue(queueName: QueueName): Queue {
  const registry = (globalQueueRegistry.__nftPlatformOperationsQueues__ ??= new Map());
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

function isQueueBackedJobType(jobType: JobDocument["type"]): jobType is QueueName {
  return queueBackedJobTypes.has(jobType as QueueName);
}

function mapBullMqStateToJobStatus(state: string): JobDocument["status"] {
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

async function listLaggingCollections(database: Db, limit: number) {
  const documents = await getMongoCollections(database)
    .collections.aggregate<
      CollectionDocument & {
        lagBlocks: number;
        indexedCheckpoint: number;
        observedCheckpoint: number;
      }
    >([...buildLaggingCollectionsPipeline(), { $limit: limit }])
    .limit(limit)
    .toArray();

  return documents.map((document) => ({
    ...serializeCollectionDocument(document),
    lagBlocks: document.lagBlocks,
    indexedCheckpoint: document.indexedCheckpoint,
    observedCheckpoint: document.observedCheckpoint
  }));
}

async function countLaggingCollections(database: Db): Promise<number> {
  const result = await getMongoCollections(database)
    .collections.aggregate<{ count: number }>([...buildLaggingCollectionsPipeline(), { $count: "count" }])
    .toArray();

  return result[0]?.count ?? 0;
}

function buildLaggingCollectionsPipeline() {
  return [
    {
      $match: {
        standard: "erc1155",
        syncStatus: { $in: ["active", "syncing"] },
        deployBlock: { $ne: null }
      }
    },
    {
      $addFields: {
        indexedCheckpoint: {
          $ifNull: ["$lastIndexedBlock", { $subtract: ["$deployBlock", 1] }]
        },
        observedCheckpoint: {
          $ifNull: ["$lastObservedBlock", "$deployBlock"]
        }
      }
    },
    {
      $addFields: {
        lagBlocks: {
          $max: [{ $subtract: ["$observedCheckpoint", "$indexedCheckpoint"] }, 0]
        }
      }
    },
    {
      $match: {
        lagBlocks: { $gt: 0 }
      }
    },
    {
      $sort: {
        lagBlocks: -1,
        updatedAt: -1,
        _id: -1
      }
    }
  ];
}