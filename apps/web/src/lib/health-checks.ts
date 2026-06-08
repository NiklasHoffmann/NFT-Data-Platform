import process from "node:process";
import { HeadBucketCommand, S3Client } from "@aws-sdk/client-s3";
import { getWebRuntimeConfig } from "./env";
import { logger } from "./logger";
import { getWebMongoDatabase } from "./mongodb";
import { getRedisClient } from "./redis";

export type DependencyHealth = {
  dependency: "mongodb" | "redis" | "storage";
  status: "ok" | "down";
  latencyMs: number;
  message: string | null;
};

const globalStorageRegistry = globalThis as typeof globalThis & {
  __nftPlatformStorageClients__?: Map<string, S3Client>;
};

const HEALTH_PROBE_TIMEOUT_MS = 2_500;

export async function probeMongoHealth(): Promise<DependencyHealth> {
  const startedAt = Date.now();

  try {
    await withTimeout(
      getWebMongoDatabase().command({ ping: 1 }),
      HEALTH_PROBE_TIMEOUT_MS,
      "mongodb"
    );

    return {
      dependency: "mongodb",
      status: "ok",
      latencyMs: Date.now() - startedAt,
      message: null
    };
  } catch (error) {
    logger.warn("health_probe_mongodb_failed", { error });

    return {
      dependency: "mongodb",
      status: "down",
      latencyMs: Date.now() - startedAt,
      message: toSafeHealthErrorMessage(error)
    };
  }
}

export async function probeRedisHealth(): Promise<DependencyHealth> {
  const startedAt = Date.now();

  try {
    const redis = getRedisClient();
    await withTimeout(
      (async () => {
        await redis.connect().catch(() => undefined);
        await redis.ping();
      })(),
      HEALTH_PROBE_TIMEOUT_MS,
      "redis"
    );

    return {
      dependency: "redis",
      status: "ok",
      latencyMs: Date.now() - startedAt,
      message: null
    };
  } catch (error) {
    logger.warn("health_probe_redis_failed", { error });

    return {
      dependency: "redis",
      status: "down",
      latencyMs: Date.now() - startedAt,
      message: toSafeHealthErrorMessage(error)
    };
  }
}

export async function probeStorageHealth(): Promise<DependencyHealth> {
  const startedAt = Date.now();
  const abortController = new AbortController();
  const timeoutHandle = setTimeout(() => abortController.abort(), HEALTH_PROBE_TIMEOUT_MS);

  try {
    const config = getWebRuntimeConfig();
    const client = getStorageClient(config);
    await client.send(
      new HeadBucketCommand({
        Bucket: config.storageBucket
      }),
      {
        abortSignal: abortController.signal
      }
    );

    return {
      dependency: "storage",
      status: "ok",
      latencyMs: Date.now() - startedAt,
      message: null
    };
  } catch (error) {
    logger.warn("health_probe_storage_failed", { error });

    return {
      dependency: "storage",
      status: "down",
      latencyMs: Date.now() - startedAt,
      message: toSafeHealthErrorMessage(error)
    };
  } finally {
    clearTimeout(timeoutHandle);
  }
}

async function withTimeout<T>(operation: Promise<T>, timeoutMs: number, dependency: string): Promise<T> {
  let timeoutHandle: NodeJS.Timeout | undefined;

  try {
    return await Promise.race([
      operation,
      new Promise<T>((_, reject) => {
        timeoutHandle = setTimeout(() => {
          reject(new Error(`${dependency}_health_timeout`));
        }, timeoutMs);
      })
    ]);
  } finally {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  }
}

function getStorageClient(config: ReturnType<typeof getWebRuntimeConfig>): S3Client {
  const registry = (globalStorageRegistry.__nftPlatformStorageClients__ ??= new Map());
  const cacheKey = `${config.storageEndpoint}|${config.storageRegion}|${config.storageBucket}|${config.storageAccessKey}`;
  const existing = registry.get(cacheKey);

  if (existing) {
    return existing;
  }

  const client = new S3Client({
    region: config.storageRegion,
    endpoint: config.storageEndpoint,
    credentials: {
      accessKeyId: config.storageAccessKey,
      secretAccessKey: config.storageSecretKey
    },
    forcePathStyle: true
  });

  registry.set(cacheKey, client);
  return client;
}

function toSafeHealthErrorMessage(error: unknown): string {
  if (process.env.NODE_ENV === "development" && error instanceof Error && error.message.trim()) {
    return error.message;
  }

  return "dependency_unavailable";
}