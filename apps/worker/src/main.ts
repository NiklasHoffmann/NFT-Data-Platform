import { dirname, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { Worker } from "bullmq";
import IORedis from "ioredis";
import {
  closeMongoClientSingleton,
  getMongoDatabase,
  initializePlatformDatabase
} from "@nft-platform/db";
import { queueNames, type QueueName } from "@nft-platform/queue";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { startChainIndexingLoop } from "./chain-indexing";
import { getWorkerRuntimeConfig } from "./env";
import { startMetadataSweepLoop } from "./metadata-sweep";
import { processQueueJob } from "./jobs/processors";

loadLocalEnvFiles({
  roots: [resolve(dirname(fileURLToPath(import.meta.url)), "../../../")]
});

async function bootstrap(): Promise<void> {
  const config = getWorkerRuntimeConfig();
  const database = getMongoDatabase({
    uri: config.mongodbUri,
    databaseName: config.mongodbDatabase,
    appName: "nft-platform-worker"
  });

  if (config.nodeEnv !== "production") {
    if (
      config.bootstrapClientId &&
      config.bootstrapApiKey &&
      config.bootstrapApiSecret &&
      config.apiClientSecretEncryptionKey
    ) {
      await initializePlatformDatabase({
        database,
        bootstrapApiClient: {
          clientId: config.bootstrapClientId,
          clientName: config.bootstrapClientId,
          apiKey: config.bootstrapApiKey,
          apiSecret: config.bootstrapApiSecret,
          scopes: config.bootstrapScopes,
          rateLimitPerMinute: config.bootstrapRateLimitPerMinute,
          allowedIps: config.bootstrapAllowedIps,
          encryptionKey: config.apiClientSecretEncryptionKey
        }
      });
    } else {
      await initializePlatformDatabase({ database });
    }
  } else {
    console.log("[worker] skipping database bootstrap in production; run npm run db:init separately for validators, indexes, and bootstrap API client sync.");
  }

  const connection = new IORedis(config.redisUrl, {
    maxRetriesPerRequest: null
  });

  const workers = Object.values(queueNames).map(
    (queueName) =>
      new Worker(
        queueName,
        async (job) => {
          const result = await processQueueJob({
            queueName,
            job,
            context: {
              database,
              redisConnection: connection,
              rpcUrls: config.rpcUrls,
              storage: config.storage,
              mediaMaxVideoBytes: config.mediaMaxVideoBytes
            }
          });

          console.log(`[worker] job processed`, {
            queueName,
            jobId: job.id,
            jobName: job.name,
            timestamp: new Date().toISOString(),
            result
          });

          return result;
        },
        {
          connection,
          concurrency: resolveQueueConcurrency(queueName, config.workerConcurrency),
          ...buildWorkerRateLimiter(config)
        }
      )
  );
  const stopMetadataSweepLoop = startMetadataSweepLoop({
    database,
    redisConnection: connection,
    config: {
      metadataSweepEnabled: config.metadataSweepEnabled,
      metadataSweepIntervalMs: config.metadataSweepIntervalMs,
      metadataSweepBatchSize: config.metadataSweepBatchSize,
      tokenMetadataTtlSeconds: config.tokenMetadataTtlSeconds,
      tokenMetadataFailureRetrySeconds: config.tokenMetadataFailureRetrySeconds,
      collectionMetadataTtlSeconds: config.collectionMetadataTtlSeconds
    }
  });
  const stopChainIndexingLoop = startChainIndexingLoop({
    database,
    redisConnection: connection,
    rpcUrls: config.rpcUrls,
    config: {
      chainIndexingEnabled: config.chainIndexingEnabled,
      chainIndexingPollIntervalMs: config.chainIndexingPollIntervalMs,
      chainIndexingBatchSize: config.chainIndexingBatchSize,
      chainIndexingMaxBlockRange: config.chainIndexingMaxBlockRange,
      chainIndexingCollectionAllowlist: config.chainIndexingCollectionAllowlist,
      chainIndexingConfirmations: config.chainIndexingConfirmations
    }
  });

  workers.forEach((worker) => {
    worker.on("ready", () => {
      console.log(`[worker] ready: ${worker.name}`);
    });

    worker.on("error", (error) => {
      console.error(`[worker] error: ${worker.name}`, error);
    });
  });

  const shutdown = async (signal: NodeJS.Signals) => {
    console.log(`[worker] shutting down on ${signal}`);
    await stopChainIndexingLoop();
    await stopMetadataSweepLoop();
    await Promise.all(workers.map((worker) => worker.close()));
    await connection.quit();
    await closeMongoClientSingleton({
      uri: config.mongodbUri,
      appName: "nft-platform-worker"
    });
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  console.log("[worker] runtime online", {
    nodeEnv: config.nodeEnv,
    concurrency: Object.fromEntries(
      Object.values(queueNames).map((queueName) => [
        queueName,
        resolveQueueConcurrency(queueName, config.workerConcurrency)
      ])
    ),
    rateLimit:
      config.workerRateLimitMax > 0
        ? { max: config.workerRateLimitMax, durationMs: config.workerRateLimitDurationMs }
        : "disabled",
    database: config.mongodbDatabase,
    mediaMaxVideoBytes: config.mediaMaxVideoBytes,
    metadataSweep: {
      enabled: config.metadataSweepEnabled,
      intervalMs: config.metadataSweepIntervalMs,
      batchSize: config.metadataSweepBatchSize,
      tokenTtlSeconds: config.tokenMetadataTtlSeconds,
      tokenFailureRetrySeconds: config.tokenMetadataFailureRetrySeconds,
      collectionTtlSeconds: config.collectionMetadataTtlSeconds
    },
    chainIndexing: {
      enabled: config.chainIndexingEnabled,
      pollIntervalMs: config.chainIndexingPollIntervalMs,
      batchSize: config.chainIndexingBatchSize,
      maxBlockRange: config.chainIndexingMaxBlockRange,
      collectionAllowlist: config.chainIndexingCollectionAllowlist
    },
    queues: Object.values(queueNames)
  });
}

/**
 * Refresh work is IO-bound - RPC calls, metadata fetches, media downloads - so running one job at
 * a time leaves the worker idle waiting on the network.
 *
 * `reindex-range` is deliberately excluded. Its jobs rewrite the ownership state of a collection
 * from a block range, and two ranges of the same collection running at once can interleave those
 * writes. Serialising the queue is what currently prevents that.
 */
function resolveQueueConcurrency(queueName: QueueName, configuredConcurrency: number): number {
  return queueName === queueNames.reindexRange ? 1 : configuredConcurrency;
}

/**
 * Caps how fast a worker pulls jobs, which is the lever for staying inside an RPC provider's rate
 * limit. Disabled unless a maximum is configured, so the default behaviour is unthrottled.
 */
function buildWorkerRateLimiter(config: {
  workerRateLimitMax: number;
  workerRateLimitDurationMs: number;
}): { limiter?: { max: number; duration: number } } {
  if (config.workerRateLimitMax <= 0) {
    return {};
  }

  return {
    limiter: {
      max: config.workerRateLimitMax,
      duration: config.workerRateLimitDurationMs
    }
  };
}

bootstrap().catch((error) => {
  console.error("[worker] bootstrap failed", error);
  process.exit(1);
});
