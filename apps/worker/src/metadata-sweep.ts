import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import type { Db } from "mongodb";
import type IORedis from "ioredis";
import {
  listStaleCollectionsForRefresh,
  listStaleTokensForRefresh,
  type StaleCollectionCandidate,
  type StaleTokenCandidate
} from "@nft-platform/db";
import {
  enqueueQueueJob,
  queueNames,
  refreshCollectionJobSchema,
  refreshTokenJobSchema
} from "@nft-platform/queue";
import type { MetadataSweepRuntimeConfig } from "./env";

type MetadataSweepLoopParams = {
  database: Db;
  redisConnection: IORedis;
  config: MetadataSweepRuntimeConfig;
};

const sweepLockKey = "metadata-sweep:lock";

/**
 * Periodically re-queues the material that has aged out of its TTL.
 *
 * Refreshes are otherwise only triggered by an explicit API call, by discovery of a missing token,
 * or by chain indexing, which means a token whose metadata changes after it was first indexed
 * (a reveal, a re-pinned IPFS document, a mutable tokenURI) would stay stale forever.
 */
export function startMetadataSweepLoop(params: MetadataSweepLoopParams): () => Promise<void> {
  if (!params.config.metadataSweepEnabled) {
    return async () => {};
  }

  const tokenQueue = new Queue(queueNames.refreshToken, { connection: params.redisConnection });
  const collectionQueue = new Queue(queueNames.refreshCollection, { connection: params.redisConnection });
  const instanceId = randomUUID();
  let stopped = false;
  let sweeping = false;

  const runSweep = async () => {
    if (stopped || sweeping) {
      return;
    }

    sweeping = true;

    try {
      await sweepOnce({ params, tokenQueue, collectionQueue, instanceId });
    } catch (error) {
      console.error("[metadata-sweep] sweep failed", error);
    } finally {
      sweeping = false;
    }
  };

  void runSweep();
  const timer = setInterval(() => {
    void runSweep();
  }, params.config.metadataSweepIntervalMs);

  timer.unref?.();

  return async () => {
    stopped = true;
    clearInterval(timer);
    await Promise.all([tokenQueue.close(), collectionQueue.close()]);
  };
}

async function sweepOnce(context: {
  params: MetadataSweepLoopParams;
  tokenQueue: Queue;
  collectionQueue: Queue;
  instanceId: string;
}): Promise<void> {
  // Every replica runs this loop, but only one of them should turn a given batch into jobs;
  // otherwise the same stale tokens are queued several times over and the RPC budget is spent
  // on duplicate work.
  if (!(await acquireSweepLock(context.params, context.instanceId))) {
    return;
  }

  const now = new Date();
  const [staleTokens, staleCollections] = await Promise.all([
    listStaleTokensForRefresh({
      database: context.params.database,
      staleBefore: subtractSeconds(now, context.params.config.tokenMetadataTtlSeconds),
      failureRetryBefore: subtractSeconds(now, context.params.config.tokenMetadataFailureRetrySeconds),
      limit: context.params.config.metadataSweepBatchSize
    }),
    listStaleCollectionsForRefresh({
      database: context.params.database,
      staleBefore: subtractSeconds(now, context.params.config.collectionMetadataTtlSeconds),
      limit: context.params.config.metadataSweepBatchSize
    })
  ]);

  const queuedTokens = await enqueueStaleTokens(context, staleTokens);
  const queuedCollections = await enqueueStaleCollections(context, staleCollections);

  if (queuedTokens > 0 || queuedCollections > 0) {
    console.log("[metadata-sweep] queued refresh work", {
      timestamp: now.toISOString(),
      staleTokens: staleTokens.length,
      queuedTokens,
      staleCollections: staleCollections.length,
      queuedCollections
    });
  }
}

async function enqueueStaleTokens(
  context: { params: MetadataSweepLoopParams; tokenQueue: Queue },
  staleTokens: StaleTokenCandidate[]
): Promise<number> {
  let queuedCount = 0;

  for (const token of staleTokens) {
    const identity = `${token.chainId}:${token.contractAddress}:${token.tokenId}`;

    // A refresh does not always move `lastMetadataFetchAt` forward - a token without a resolvable
    // metadata URI, for instance, stays exactly as stale as it was. The claim marker keeps such a
    // token from being re-queued on every single tick.
    if (!(await claimSweepSlot(context.params, `sweep:token:${identity}`))) {
      continue;
    }

    await enqueueQueueJob({
      queue: context.tokenQueue,
      queueName: queueNames.refreshToken,
      // Parsed through the same schema the API routes use, so that an identical refresh
      // produces an identical idempotency key no matter which side queued it.
      payload: refreshTokenJobSchema.parse({
        chainId: token.chainId,
        contractAddress: token.contractAddress,
        tokenId: token.tokenId,
        forceMetadata: false,
        forceOwnership: false
      }),
      allowReenqueueCompleted: true
    });

    queuedCount += 1;
  }

  return queuedCount;
}

async function enqueueStaleCollections(
  context: { params: MetadataSweepLoopParams; collectionQueue: Queue },
  staleCollections: StaleCollectionCandidate[]
): Promise<number> {
  let queuedCount = 0;

  for (const collection of staleCollections) {
    const identity = `${collection.chainId}:${collection.contractAddress}`;

    if (!(await claimSweepSlot(context.params, `sweep:collection:${identity}`))) {
      continue;
    }

    await enqueueQueueJob({
      queue: context.collectionQueue,
      queueName: queueNames.refreshCollection,
      payload: refreshCollectionJobSchema.parse({
        chainId: collection.chainId,
        contractAddress: collection.contractAddress,
        standard: collection.standard,
        fullRescan: false
      }),
      allowReenqueueCompleted: true
    });

    queuedCount += 1;
  }

  return queuedCount;
}

/**
 * Held only for the length of one sweep interval, so a replica that dies mid-sweep does not block
 * the others beyond the next tick.
 */
async function acquireSweepLock(params: MetadataSweepLoopParams, instanceId: string): Promise<boolean> {
  const lockTtlSeconds = Math.max(Math.ceil(params.config.metadataSweepIntervalMs / 1000), 1);
  const claim = await params.redisConnection.set(sweepLockKey, instanceId, "EX", lockTtlSeconds, "NX");

  return claim === "OK";
}

async function claimSweepSlot(params: MetadataSweepLoopParams, key: string): Promise<boolean> {
  const claim = await params.redisConnection.set(
    key,
    "1",
    "EX",
    params.config.tokenMetadataTtlSeconds,
    "NX"
  );

  return claim === "OK";
}

function subtractSeconds(reference: Date, seconds: number): Date {
  return new Date(reference.getTime() - seconds * 1000);
}
