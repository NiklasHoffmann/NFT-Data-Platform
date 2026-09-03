import { randomUUID } from "node:crypto";
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

const indexingLockKey = "chain-indexing:lock";

type ChainIndexingLoopParams = {
  database: Db;
  redisConnection: IORedis;
  rpcUrls: Record<number, string>;
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
  // Every replica runs this loop. Without a lock they would each read the chain head and queue the
  // same reindex ranges, multiplying RPC cost and racing each other on the progress writes.
  if (!(await acquireIndexingLock(params))) {
    return;
  }

  const collections = await listCollectionsForAutoIndexing({
    database: params.database,
    collectionAllowlist: params.config.chainIndexingCollectionAllowlist,
    limit: params.config.chainIndexingBatchSize
  });

  // The chain head is a property of the chain, not of the collection. Reading it once per chain
  // turns N identical eth_blockNumber calls per tick into one.
  const latestBlockByChain = await readLatestBlockPerChain({
    chainIds: [...new Set(collections.map((collection) => collection.chainId))],
    rpcUrls: params.rpcUrls
  });

  for (const collection of collections) {
    const latestBlock = latestBlockByChain.get(collection.chainId);

    if (latestBlock === undefined || latestBlock === null) {
      continue;
    }

    await maybeQueueReindexForCollection({
      collection,
      database: params.database,
      queue,
      latestBlock,
      confirmations: params.config.chainIndexingConfirmations,
      maxBlockRange: params.config.chainIndexingMaxBlockRange
    });
  }
}

/**
 * Held for one poll interval, so a replica that dies mid-poll frees the loop by the next tick.
 */
async function acquireIndexingLock(params: ChainIndexingLoopParams): Promise<boolean> {
  const lockTtlSeconds = Math.max(Math.ceil(params.config.chainIndexingPollIntervalMs / 1000), 1);
  const claim = await params.redisConnection.set(
    indexingLockKey,
    randomUUID(),
    "EX",
    lockTtlSeconds,
    "NX"
  );

  return claim === "OK";
}

async function readLatestBlockPerChain(params: {
  chainIds: number[];
  rpcUrls: Record<number, string>;
}): Promise<Map<number, number | null>> {
  const entries = await Promise.all(
    params.chainIds.map(
      async (chainId): Promise<[number, number | null]> => [
        chainId,
        await readLatestBlock({ chainId, rpcUrls: params.rpcUrls })
      ]
    )
  );

  return new Map(entries);
}

/**
 * Works out which block range may safely be reindexed next, or null when there is nothing to do.
 *
 * Indexing right up to the chain head means indexing blocks that can still be reorged away.
 * Transfers read from an orphaned block would be written into the read model and never corrected,
 * because the reindex job for that range is deduplicated by its idempotency key - a second attempt
 * at the same range is silently dropped. Staying a confirmation depth behind the head avoids the
 * situation rather than trying to detect and repair it.
 */
export function resolveReindexWindow(params: {
  latestBlock: number;
  lastIndexedBlock: number | null;
  deployBlock: number | null;
  confirmations: number;
  maxBlockRange: number;
}): { fromBlock: number; toBlock: number } | null {
  // Clamped at zero so a chain younger than the confirmation depth cannot produce a negative
  // block number, which the reindex payload schema rejects.
  const safeLatestBlock = Math.max(params.latestBlock - params.confirmations, 0);
  const fromBlock =
    params.lastIndexedBlock !== null ? params.lastIndexedBlock + 1 : params.deployBlock;

  if (fromBlock === null || safeLatestBlock < fromBlock) {
    return null;
  }

  return {
    fromBlock,
    toBlock: Math.min(safeLatestBlock, fromBlock + params.maxBlockRange - 1)
  };
}

async function maybeQueueReindexForCollection(params: {
  collection: CollectionDocument;
  database: Db;
  queue: Queue;
  latestBlock: number;
  confirmations: number;
  maxBlockRange: number;
}): Promise<void> {
  const now = new Date();
  const window = resolveReindexWindow({
    latestBlock: params.latestBlock,
    lastIndexedBlock: params.collection.lastIndexedBlock,
    deployBlock: params.collection.deployBlock,
    confirmations: params.confirmations,
    maxBlockRange: params.maxBlockRange
  });

  if (!window) {
    // lastObservedBlock records the true head, so the operator view still shows the real lag.
    if (params.collection.lastObservedBlock !== params.latestBlock) {
      await persistCollectionProgress({
        collection: params.collection,
        database: params.database,
        lastObservedBlock: params.latestBlock,
        syncStatus: "active",
        updatedAt: now
      });
    }

    return;
  }

  const payload: ReindexRangeJob = {
    chainId: params.collection.chainId,
    contractAddress: params.collection.contractAddress,
    fromBlock: window.fromBlock,
    toBlock: window.toBlock
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
    lastObservedBlock: params.latestBlock,
    syncStatus: status === "failed" ? "error" : "syncing",
    updatedAt: now
  });
}

async function readLatestBlock(params: {
  chainId: number;
  rpcUrls: Record<number, string>;
}): Promise<number | null> {
  const rpcUrl = params.rpcUrls[params.chainId];

  if (!rpcUrl) {
    console.warn(`[chain-indexing] no RPC URL configured for chainId ${params.chainId}, skipping`);
    return null;
  }

  const publicClient = createChainPublicClient({
    chainId: params.chainId,
    rpcUrl
  });

  return publicClient
    .getBlockNumber()
    .then((value) => Number(value))
    .catch((error) => {
      console.error("[chain-indexing] failed to read latest block", {
        chainId: params.chainId,
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