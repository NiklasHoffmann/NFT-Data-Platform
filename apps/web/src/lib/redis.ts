import IORedis from "ioredis";
import { getWebRuntimeConfig } from "./env";

const globalRedisRegistry = globalThis as typeof globalThis & {
  __nftPlatformRedisClients__?: Map<string, IORedis>;
};

export function getRedisClient(): IORedis {
  const config = getWebRuntimeConfig();
  const registry = (globalRedisRegistry.__nftPlatformRedisClients__ ??= new Map());
  const existingClient = registry.get(config.redisUrl);

  if (existingClient) {
    return existingClient;
  }

  const client = new IORedis(config.redisUrl, {
    maxRetriesPerRequest: null,
    lazyConnect: true
  });

  registry.set(config.redisUrl, client);
  return client;
}