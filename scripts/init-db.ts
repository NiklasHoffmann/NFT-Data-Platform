import { existsSync, readFileSync } from "node:fs";
import process from "node:process";
import {
  closeMongoClientSingleton,
  getMongoDatabase,
  initializePlatformDatabase
} from "@nft-platform/db";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { parseScopeList } from "@nft-platform/security";

loadLocalEnvFiles();

function main(): Promise<void> {
  const mongodbUri = process.env.MONGODB_URI ?? "mongodb://localhost:27017";
  const mongodbDatabase = process.env.MONGODB_DATABASE ?? "nft_data_platform";
  const bootstrapClientId = process.env.API_BOOTSTRAP_CLIENT_ID ?? "";
  const bootstrapApiKey = process.env.API_BOOTSTRAP_KEY ?? "";
  const bootstrapApiSecret = process.env.API_BOOTSTRAP_SECRET ?? "";
  const bootstrapScopes = parseScopeList(process.env.API_BOOTSTRAP_SCOPES ?? "");
  const bootstrapRateLimitPerMinute = Number(
    process.env.API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE ?? "300"
  );
  const bootstrapAllowedIps = parseCsvList(process.env.API_BOOTSTRAP_ALLOWED_IPS ?? "");
  const apiClientSecretEncryptionKey = process.env.API_CLIENT_SECRET_ENCRYPTION_KEY ?? "";

  if (!apiClientSecretEncryptionKey) {
    throw new Error("Missing API_CLIENT_SECRET_ENCRYPTION_KEY for db bootstrap.");
  }

  const database = getMongoDatabase({
    uri: mongodbUri,
    databaseName: mongodbDatabase,
    appName: "nft-platform-db-init"
  });

  const bootstrapApiClient =
    bootstrapClientId && bootstrapApiKey && bootstrapApiSecret
      ? {
          clientId: bootstrapClientId,
          clientName: bootstrapClientId,
          apiKey: bootstrapApiKey,
          apiSecret: bootstrapApiSecret,
          scopes: bootstrapScopes,
          rateLimitPerMinute: Number.isFinite(bootstrapRateLimitPerMinute)
            ? bootstrapRateLimitPerMinute
            : 300,
          allowedIps: bootstrapAllowedIps,
          encryptionKey: apiClientSecretEncryptionKey
        }
      : undefined;

  return initializePlatformDatabase({
    database,
    bootstrapApiClient
  })
    .then(() => {
      console.log("[db:init] MongoDB validators and indexes ensured.");

      if (bootstrapApiClient) {
        console.log("[db:init] Bootstrap API client upserted.");
      }
    })
    .finally(async () => {
      await closeMongoClientSingleton({
        uri: mongodbUri,
        appName: "nft-platform-db-init"
      });
    });
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

main().catch((error) => {
  console.error("[db:init] failed", error);
  process.exit(1);
});
