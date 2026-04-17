import { Queue } from "bullmq";
import type { Db } from "mongodb";
import type IORedis from "ioredis";
import { createChainPublicClient } from "@nft-platform/chain";
import {
  createJob,
  listCollectionsForAutoIndexing,
  type CollectionDocument,
  upsertCollection
} from "@nft-platform/db";
import {
  buildIdempotencyKey,
  queueNames,
  type ReindexRangeJob
} from "@nft-platform/queue";
import type { ChainIndexingRuntimeConfig } from "./env";

type ChainIndexingLoopParams = {
  database: Db;
  redisConnection: IORedis;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
  config: ChainIndexingRuntimeConfig;
};

export function startChainIndexingLoop(params: ChainIndexingLoopParams): () => Promise<void> {
  if (!params.config.chainIndexingEnabled) {
    return async () => {};
  }

  const queue = new Queue(queueNames.reindexRange, {
    connection: params.redisConnection
  });
  let stopped = false;
  let polling = false;

  const runPoll = async () => {
    if (stopped || polling) {
      return;
    }

    polling = true;

    try {
      await pollCollections(params, queue);
    } catch (error) {
      console.error("[chain-indexing] poll failed", error);
    } finally {
      polling = false;
    }
  };

  void runPoll();
  const timer = setInterval(() => {
    void runPoll();
  }, params.config.chainIndexingPollIntervalMs);

  timer.unref?.();

  return async () => {
    stopped = true;
    clearInterval(timer);
    await queue.close();
  };
}

async function pollCollections(params: ChainIndexingLoopParams, queue: Queue): Promise<void> {
  const collections = await listCollectionsForAutoIndexing({
    database: params.database,
    collectionAllowlist: params.config.chainIndexingCollectionAllowlist,
    limit: params.config.chainIndexingBatchSize
  });

  for (const collection of collections) {
    await maybeQueueReindexForCollection({
      collection,
      database: params.database,
      queue,
      rpcMainnetUrl: params.rpcMainnetUrl,
      rpcSepoliaUrl: params.rpcSepoliaUrl,
      maxBlockRange: params.config.chainIndexingMaxBlockRange
    });
  }
}

async function maybeQueueReindexForCollection(params: {
  collection: CollectionDocument;
  database: Db;
  queue: Queue;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
  maxBlockRange: number;
}): Promise<void> {
  const now = new Date();
  const latestBlock = await readLatestBlock({
    collection: params.collection,
    rpcMainnetUrl: params.rpcMainnetUrl,
    rpcSepoliaUrl: params.rpcSepoliaUrl
  });

  if (latestBlock === null) {
    return;
  }

  const fromBlock =
    params.collection.lastIndexedBlock !== null
      ? params.collection.lastIndexedBlock + 1
      : params.collection.deployBlock;

  if (fromBlock === null || latestBlock < fromBlock) {
    if (params.collection.lastObservedBlock !== latestBlock) {
      await persistCollectionProgress({
        collection: params.collection,
        database: params.database,
        lastObservedBlock: latestBlock,
        syncStatus: "active",
        updatedAt: now
      });
    }

    return;
  }

  const toBlock = Math.min(latestBlock, fromBlock + params.maxBlockRange - 1);
  const payload: ReindexRangeJob = {
    chainId: params.collection.chainId,
    contractAddress: params.collection.contractAddress,
    fromBlock,
    toBlock
  };
  const queueJobId = buildIdempotencyKey(queueNames.reindexRange, payload);
  const existingJob = await params.queue.getJob(queueJobId);

  if (!existingJob) {
    await params.queue.add(queueNames.reindexRange, payload, {
      jobId: queueJobId,
      removeOnComplete: 500,
      removeOnFail: 500
    });
  }

  const state = existingJob ? await existingJob.getState() : "waiting";
  const status = mapBullMqStateToJobStatus(state);

  await createJob(params.database, {
    queueJobId,
    type: "reindex-range",
    payload,
    status,
    attempts: existingJob?.attemptsMade ?? 0,
    lastError: null,
    createdAt: now,
    updatedAt: now
  });

  await persistCollectionProgress({
    collection: params.collection,
    database: params.database,
    lastObservedBlock: latestBlock,
    syncStatus: status === "failed" ? "error" : "syncing",
    updatedAt: now
  });
}

async function readLatestBlock(params: {
  collection: CollectionDocument;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
}): Promise<number | null> {
  const rpcUrl = params.collection.chainId === 1 ? params.rpcMainnetUrl : params.rpcSepoliaUrl;
  const publicClient = createChainPublicClient({
    chainId: params.collection.chainId,
    rpcUrl
  });

  return publicClient
    .getBlockNumber()
    .then((value) => Number(value))
    .catch((error) => {
      console.error("[chain-indexing] failed to read latest block", {
        chainId: params.collection.chainId,
        contractAddress: params.collection.contractAddress,
        error
      });
      return null;
    });
}

async function persistCollectionProgress(params: {
  collection: CollectionDocument;
  database: Db;
  lastObservedBlock: number;
  syncStatus: CollectionDocument["syncStatus"];
  updatedAt: Date;
}): Promise<void> {
  await upsertCollection(params.database, {
    chainId: params.collection.chainId,
    contractAddress: params.collection.contractAddress,
    standard: params.collection.standard,
    name: params.collection.name,
    symbol: params.collection.symbol,
    baseUri: params.collection.baseUri,
    contractUriRaw: params.collection.contractUriRaw,
    contractUriResolved: params.collection.contractUriResolved,
    creatorName: params.collection.creatorName,
    creatorAddress: params.collection.creatorAddress,
    contractOwnerAddress: params.collection.contractOwnerAddress,
    royaltyRecipientAddress: params.collection.royaltyRecipientAddress,
    royaltyBasisPoints: params.collection.royaltyBasisPoints,
    collectionMetadataPayload: params.collection.collectionMetadataPayload,
    collectionMetadataHash: params.collection.collectionMetadataHash,
    lastCollectionMetadataFetchAt: params.collection.lastCollectionMetadataFetchAt,
    lastCollectionMetadataError: params.collection.lastCollectionMetadataError,
    description: params.collection.description,
    externalUrl: params.collection.externalUrl,
    imageOriginalUrl: params.collection.imageOriginalUrl,
    bannerImageOriginalUrl: params.collection.bannerImageOriginalUrl,
    featuredImageOriginalUrl: params.collection.featuredImageOriginalUrl,
    animationOriginalUrl: params.collection.animationOriginalUrl,
    audioOriginalUrl: params.collection.audioOriginalUrl,
    interactiveOriginalUrl: params.collection.interactiveOriginalUrl,
    totalSupply: params.collection.totalSupply,
    indexedTokenCount: params.collection.indexedTokenCount,
    deployBlock: params.collection.deployBlock,
    lastObservedBlock: params.lastObservedBlock,
    lastIndexedBlock: params.collection.lastIndexedBlock,
    syncStatus: params.syncStatus,
    lastSyncAt: params.updatedAt,
    createdAt: params.collection.createdAt,
    updatedAt: params.updatedAt
  });
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