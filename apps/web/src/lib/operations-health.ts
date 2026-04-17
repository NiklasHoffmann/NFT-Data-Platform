import { Queue } from "bullmq";
import type { Db } from "mongodb";
import { queueNames, type QueueName } from "@nft-platform/queue";
import {
  getMongoCollections,
  serializeCollectionDocument,
  serializeJobDocument,
  type CollectionDocument,
  type JobDocument
} from "@nft-platform/db";
import { getRedisClient } from "./redis";

type SerializedJob = ReturnType<typeof serializeJobDocument>;
const queueBackedJobTypes = new Set<QueueName>(Object.values(queueNames));
const globalQueueRegistry = globalThis as typeof globalThis & {
  __nftPlatformOperationsQueues__?: Map<QueueName, Queue>;
};

export type OperationsHealthSnapshot = {
  summary: {
    activeJobsCount: number;
    retryingMediaJobsCount: number;
    failedJobsCount: number;
    laggingCollectionsCount: number;
  };
  activeJobs: SerializedJob[];
  retryingMediaJobs: SerializedJob[];
  recentFailedJobs: SerializedJob[];
  laggingCollections: Array<
    ReturnType<typeof serializeCollectionDocument> & {
      lagBlocks: number;
      indexedCheckpoint: number;
      observedCheckpoint: number;
    }
  >;
};

export async function loadOperationsHealth(database: Db): Promise<OperationsHealthSnapshot> {
  const collections = getMongoCollections(database);
  const liveActiveJobs = await listLiveQueueJobs(database);
  const retryingMediaJobs = liveActiveJobs.filter((job) => job.type === "refresh-media" && job.attempts > 1);

  const [
    recentFailedJobs,
    laggingCollections,
    failedJobsCount,
    laggingCollectionsCount
  ] = await Promise.all([
    collections.jobs
      .find({ status: "failed" })
      .sort({ updatedAt: -1, _id: -1 })
      .limit(6)
      .toArray(),
    listLaggingCollections(database, 6),
    collections.jobs.countDocuments({ status: "failed" }),
    countLaggingCollections(database)
  ]);

  return {
    summary: {
      activeJobsCount: liveActiveJobs.length,
      retryingMediaJobsCount: retryingMediaJobs.length,
      failedJobsCount,
      laggingCollectionsCount
    },
    activeJobs: liveActiveJobs.slice(0, 8).map(serializeJobDocument),
    retryingMediaJobs: retryingMediaJobs.slice(0, 6).map(serializeJobDocument),
    recentFailedJobs: recentFailedJobs.map(serializeJobDocument),
    laggingCollections
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