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
import { queueNames } from "@nft-platform/queue";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { startChainIndexingLoop } from "./chain-indexing";
import { getWorkerRuntimeConfig } from "./env";
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
              rpcMainnetUrl: config.rpcMainnetUrl,
              rpcSepoliaUrl: config.rpcSepoliaUrl,
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
        { connection }
      )
  );
  const stopChainIndexingLoop = startChainIndexingLoop({
    database,
    redisConnection: connection,
    rpcMainnetUrl: config.rpcMainnetUrl,
    rpcSepoliaUrl: config.rpcSepoliaUrl,
    config: {
      chainIndexingEnabled: config.chainIndexingEnabled,
      chainIndexingPollIntervalMs: config.chainIndexingPollIntervalMs,
      chainIndexingBatchSize: config.chainIndexingBatchSize,
      chainIndexingMaxBlockRange: config.chainIndexingMaxBlockRange,
      chainIndexingCollectionAllowlist: config.chainIndexingCollectionAllowlist
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
    database: config.mongodbDatabase,
    mediaMaxVideoBytes: config.mediaMaxVideoBytes,
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

bootstrap().catch((error) => {
  console.error("[worker] bootstrap failed", error);
  process.exit(1);
});
