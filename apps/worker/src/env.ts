import { normalizeContractAddress, type Scope } from "@nft-platform/domain";
import { storageConfigSchema, type StorageConfig } from "@nft-platform/storage";
import { parseScopeList } from "@nft-platform/security";
import { z } from "zod";

const workerRuntimeConfigSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  MONGODB_URI: z.string().min(1).default("mongodb://localhost:27017"),
  MONGODB_DATABASE: z.string().min(1).default("nft_data_platform"),
  RPC_MAINNET_URL: z.string().url(),
  RPC_SEPOLIA_URL: z.string().url(),
  S3_ENDPOINT: z.string().url(),
  S3_REGION: z.string().min(1),
  S3_ACCESS_KEY: z.string().min(1),
  S3_SECRET_KEY: z.string().min(1),
  S3_BUCKET: z.string().min(1),
  S3_PUBLIC_BASE_URL: z.string().url(),
  MEDIA_MAX_VIDEO_BYTES: z.coerce.number().int().positive().default(25_000_000),
  CHAIN_INDEXING_ENABLED: z.string().default("false"),
  CHAIN_INDEXING_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30_000),
  CHAIN_INDEXING_BATCH_SIZE: z.coerce.number().int().positive().default(10),
  CHAIN_INDEXING_MAX_BLOCK_RANGE: z.coerce.number().int().positive().default(2_000),
  CHAIN_INDEXING_COLLECTION_ALLOWLIST: z.string().default(""),
  API_CLIENT_SECRET_ENCRYPTION_KEY: z.string().default(""),
  API_BOOTSTRAP_CLIENT_ID: z.string().default(""),
  API_BOOTSTRAP_KEY: z.string().default(""),
  API_BOOTSTRAP_SECRET: z.string().default(""),
  API_BOOTSTRAP_SCOPES: z.string().default(""),
  API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().positive().default(300),
  API_BOOTSTRAP_ALLOWED_IPS: z.string().default("")
});

export type WorkerRuntimeConfig = {
  nodeEnv: "development" | "test" | "production";
  redisUrl: string;
  mongodbUri: string;
  mongodbDatabase: string;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
  storage: StorageConfig;
  mediaMaxVideoBytes: number;
  chainIndexingEnabled: boolean;
  chainIndexingPollIntervalMs: number;
  chainIndexingBatchSize: number;
  chainIndexingMaxBlockRange: number;
  chainIndexingCollectionAllowlist: Array<{
    chainId: number;
    contractAddress: string;
  }>;
  apiClientSecretEncryptionKey: string;
  bootstrapClientId: string;
  bootstrapApiKey: string;
  bootstrapApiSecret: string;
  bootstrapScopes: Scope[];
  bootstrapRateLimitPerMinute: number;
  bootstrapAllowedIps: string[];
};

export function getWorkerRuntimeConfig(): WorkerRuntimeConfig {
  const parsed = workerRuntimeConfigSchema.parse(process.env);

  return {
    nodeEnv: parsed.NODE_ENV,
    redisUrl: parsed.REDIS_URL,
    mongodbUri: parsed.MONGODB_URI,
    mongodbDatabase: parsed.MONGODB_DATABASE,
    rpcMainnetUrl: parsed.RPC_MAINNET_URL,
    rpcSepoliaUrl: parsed.RPC_SEPOLIA_URL,
    storage: storageConfigSchema.parse({
      endpoint: parsed.S3_ENDPOINT,
      region: parsed.S3_REGION,
      accessKey: parsed.S3_ACCESS_KEY,
      secretKey: parsed.S3_SECRET_KEY,
      bucket: parsed.S3_BUCKET,
      publicBaseUrl: parsed.S3_PUBLIC_BASE_URL
    }),
    mediaMaxVideoBytes: parsed.MEDIA_MAX_VIDEO_BYTES,
    chainIndexingEnabled: parsed.CHAIN_INDEXING_ENABLED.trim().toLowerCase() === "true",
    chainIndexingPollIntervalMs: parsed.CHAIN_INDEXING_POLL_INTERVAL_MS,
    chainIndexingBatchSize: parsed.CHAIN_INDEXING_BATCH_SIZE,
    chainIndexingMaxBlockRange: parsed.CHAIN_INDEXING_MAX_BLOCK_RANGE,
    chainIndexingCollectionAllowlist: parseCollectionAllowlist(parsed.CHAIN_INDEXING_COLLECTION_ALLOWLIST),
    apiClientSecretEncryptionKey: parsed.API_CLIENT_SECRET_ENCRYPTION_KEY,
    bootstrapClientId: parsed.API_BOOTSTRAP_CLIENT_ID,
    bootstrapApiKey: parsed.API_BOOTSTRAP_KEY,
    bootstrapApiSecret: parsed.API_BOOTSTRAP_SECRET,
    bootstrapScopes: parseScopeList(parsed.API_BOOTSTRAP_SCOPES),
    bootstrapRateLimitPerMinute: parsed.API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE,
    bootstrapAllowedIps: parseCsvList(parsed.API_BOOTSTRAP_ALLOWED_IPS)
  };
}

function parseCsvList(value: string): string[] {
  if (!value.trim()) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseCollectionAllowlist(value: string): Array<{ chainId: number; contractAddress: string }> {
  if (!value.trim()) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const separatorIndex = entry.indexOf(":");

      if (separatorIndex <= 0 || separatorIndex === entry.length - 1) {
        throw new Error(
          `Invalid CHAIN_INDEXING_COLLECTION_ALLOWLIST entry \"${entry}\". Expected \"<chainId>:<contractAddress>\".`
        );
      }

      const chainId = Number(entry.slice(0, separatorIndex).trim());
      const contractAddress = entry.slice(separatorIndex + 1).trim();

      if (!Number.isInteger(chainId) || chainId <= 0) {
        throw new Error(
          `Invalid CHAIN_INDEXING_COLLECTION_ALLOWLIST chainId in entry \"${entry}\". Expected a positive integer.`
        );
      }

      if (!/^0x[a-fA-F0-9]{40}$/.test(contractAddress)) {
        throw new Error(
          `Invalid CHAIN_INDEXING_COLLECTION_ALLOWLIST contractAddress in entry \"${entry}\". Expected a valid EVM address.`
        );
      }

      return {
        chainId,
        contractAddress: normalizeContractAddress(contractAddress)
      };
    });
}

export type ChainIndexingRuntimeConfig = Pick<
  WorkerRuntimeConfig,
  | "chainIndexingEnabled"
  | "chainIndexingPollIntervalMs"
  | "chainIndexingBatchSize"
  | "chainIndexingMaxBlockRange"
  | "chainIndexingCollectionAllowlist"
>;
