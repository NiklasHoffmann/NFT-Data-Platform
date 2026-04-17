import { dirname, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import type { Scope } from "@nft-platform/domain";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { parseScopeList } from "@nft-platform/security";
import { z } from "zod";

loadLocalEnvFiles({
  roots: [resolve(dirname(fileURLToPath(import.meta.url)), "../../../../")]
});

const webRuntimeConfigSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  APP_BASE_URL: z.string().url().default("http://localhost:3000"),
  S3_PUBLIC_BASE_URL: z.string().url().default("http://localhost:9000/nft-media"),
  MONGODB_URI: z.string().min(1).default("mongodb://localhost:27017"),
  MONGODB_DATABASE: z.string().min(1).default("nft_data_platform"),
  REDIS_URL: z.string().min(1).default("redis://localhost:6379"),
  API_CLIENT_SECRET_ENCRYPTION_KEY: z.string().default(""),
  API_BOOTSTRAP_CLIENT_ID: z.string().default(""),
  API_BOOTSTRAP_KEY: z.string().default(""),
  API_BOOTSTRAP_SECRET: z.string().default(""),
  API_BOOTSTRAP_SCOPES: z.string().default(""),
  API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE: z.coerce.number().int().positive().default(300),
  API_BOOTSTRAP_ALLOWED_IPS: z.string().default(""),
  AUTH_MAX_TIMESTAMP_SKEW_SEC: z.coerce.number().int().positive().default(300)
});

export type WebRuntimeConfig = {
  nodeEnv: "development" | "test" | "production";
  appBaseUrl: string;
  mediaPublicBaseUrl: string;
  mongodbUri: string;
  mongodbDatabase: string;
  redisUrl: string;
  apiClientSecretEncryptionKey: string;
  bootstrapClientId: string;
  bootstrapApiKey: string;
  bootstrapApiSecret: string;
  bootstrapScopes: Scope[];
  bootstrapRateLimitPerMinute: number;
  bootstrapAllowedIps: string[];
  authMaxTimestampSkewSec: number;
};

export function getWebRuntimeConfig(): WebRuntimeConfig {
  const parsed = webRuntimeConfigSchema.parse(process.env);

  return {
    nodeEnv: parsed.NODE_ENV,
    appBaseUrl: parsed.APP_BASE_URL,
    mediaPublicBaseUrl: parsed.S3_PUBLIC_BASE_URL,
    mongodbUri: parsed.MONGODB_URI,
    mongodbDatabase: parsed.MONGODB_DATABASE,
    redisUrl: parsed.REDIS_URL,
    apiClientSecretEncryptionKey: parsed.API_CLIENT_SECRET_ENCRYPTION_KEY,
    bootstrapClientId: parsed.API_BOOTSTRAP_CLIENT_ID,
    bootstrapApiKey: parsed.API_BOOTSTRAP_KEY,
    bootstrapApiSecret: parsed.API_BOOTSTRAP_SECRET,
    bootstrapScopes: parseScopeList(parsed.API_BOOTSTRAP_SCOPES),
    bootstrapRateLimitPerMinute: parsed.API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE,
    bootstrapAllowedIps: parseCsvList(parsed.API_BOOTSTRAP_ALLOWED_IPS),
    authMaxTimestampSkewSec: parsed.AUTH_MAX_TIMESTAMP_SKEW_SEC
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
