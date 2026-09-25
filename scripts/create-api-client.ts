import { randomBytes } from "node:crypto";
import process from "node:process";
import {
  closeMongoClientSingleton,
  findApiClientByClientId,
  getMongoDatabase,
  upsertApiClient
} from "@nft-platform/db";
import { apiClientStatusSchema, scopeSchema } from "@nft-platform/domain";
import type { Scope } from "@nft-platform/domain";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { buildApiKeyPrefix, encryptSecret, sha256Hex } from "@nft-platform/security";
import { z } from "zod";

loadLocalEnvFiles();

const appName = "nft-platform-create-api-client";

const booleanFlags = new Set(["rotate", "help"]);

const optionsSchema = z
  .object({
    help: z.boolean().default(false),
    "client-id": z.string().min(1),
    name: z.string().min(1).optional(),
    scopes: z.string().min(1),
    "rate-limit": z.coerce.number().int().positive().default(300),
    "allowed-ips": z.string().default(""),
    "api-key": z.string().min(16).optional(),
    "api-secret": z.string().min(16).optional(),
    status: apiClientStatusSchema.default("active"),
    rotate: z.boolean().default(false)
  })
  .strict();

const usage = `
Usage: npm run api:create-client -- --client-id <id> --scopes <csv> [options]

Creates an API client for /api/v1. The secret is stored encrypted and cannot be
read back afterwards, so the credentials are printed once and only once.

Required
  --client-id <id>        Stable identifier, also used for rate limiting and audit logs.
  --scopes <csv>          Comma separated. Valid scopes:
                          ${scopeSchema.options.join(", ")}

Optional
  --name <name>           Human readable name. Defaults to the client id.
  --rate-limit <n>        Requests per minute for this client. Default 300.
  --allowed-ips <csv>     Restrict the client to these source IPs. Default: no restriction.
  --api-key <key>         Supply a key instead of generating one.
  --api-secret <secret>   Supply a secret instead of generating one.
  --status <status>       ${apiClientStatusSchema.options.join(" | ")}. Default active.
  --rotate                Replace key and secret of an existing client. Without it an
                          existing client id is refused, so a repeated run cannot
                          silently lock out a running consumer.

Requires API_CLIENT_SECRET_ENCRYPTION_KEY in the environment.
`.trim();

type ParsedArguments = Record<string, string | boolean>;

function parseArguments(argv: readonly string[]): ParsedArguments {
  const parsed: ParsedArguments = {};

  for (let index = 0; index < argv.length; index += 1) {
    const entry = argv[index];

    if (entry === undefined || !entry.startsWith("--")) {
      continue;
    }

    const withoutDashes = entry.slice(2);
    const equalsAt = withoutDashes.indexOf("=");

    if (equalsAt !== -1) {
      parsed[withoutDashes.slice(0, equalsAt)] = withoutDashes.slice(equalsAt + 1);
      continue;
    }

    if (booleanFlags.has(withoutDashes)) {
      parsed[withoutDashes] = true;
      continue;
    }

    const value = argv[index + 1];

    if (value === undefined || value.startsWith("--")) {
      throw new Error(`Option --${withoutDashes} needs a value.`);
    }

    parsed[withoutDashes] = value;
    index += 1;
  }

  return parsed;
}

function parseScopes(value: string): Scope[] {
  const entries = splitCsv(value);
  const result = z.array(scopeSchema).min(1).safeParse(entries);

  if (!result.success) {
    const unknown = entries.filter((entry) => !scopeSchema.options.some((scope) => scope === entry));

    throw new Error(
      unknown.length > 0
        ? `Unknown scope(s): ${unknown.join(", ")}. Valid scopes: ${scopeSchema.options.join(", ")}`
        : `--scopes needs at least one of: ${scopeSchema.options.join(", ")}`
    );
  }

  return result.data;
}

function splitCsv(value: string): string[] {
  if (!value.trim()) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

// Prefixed so the key is recognizable in logs and by secret scanners. Only the
// first 12 characters are stored as keyPrefix, which is what identifies the
// client in the audit log.
function generateApiKey(): string {
  return `nft_${randomBytes(24).toString("base64url")}`;
}

function generateApiSecret(): string {
  return randomBytes(48).toString("base64url");
}

async function main(): Promise<void> {
  const args = parseArguments(process.argv.slice(2));

  // Before validation: --help must work on its own, without the required options.
  if (args.help === true) {
    console.log(usage);
    return;
  }

  const options = optionsSchema.parse(args);

  const encryptionKey = process.env.API_CLIENT_SECRET_ENCRYPTION_KEY ?? "";

  if (!encryptionKey) {
    throw new Error(
      "Missing API_CLIENT_SECRET_ENCRYPTION_KEY. Without it the secret cannot be encrypted, and a client written now would be unreadable for the API."
    );
  }

  const clientId = options["client-id"];
  const scopes = parseScopes(options.scopes);
  const allowedIps = splitCsv(options["allowed-ips"]);
  const apiKey = options["api-key"] ?? generateApiKey();
  const apiSecret = options["api-secret"] ?? generateApiSecret();
  const generated = options["api-key"] === undefined && options["api-secret"] === undefined;

  const mongodbUri = process.env.MONGODB_URI ?? "mongodb://localhost:27017";
  const mongodbDatabase = process.env.MONGODB_DATABASE ?? "nft_data_platform";

  const database = getMongoDatabase({
    uri: mongodbUri,
    databaseName: mongodbDatabase,
    appName
  });

  try {
    const existing = await findApiClientByClientId({ database, clientId });

    if (existing && !options.rotate) {
      throw new Error(
        `Client "${clientId}" already exists. Re-run with --rotate to replace its key and secret — every consumer using the current credentials stops working the moment you do.`
      );
    }

    const timestamp = new Date();

    await upsertApiClient(database, {
      clientId,
      clientName: options.name ?? clientId,
      keyPrefix: buildApiKeyPrefix(apiKey),
      keyHash: sha256Hex(apiKey),
      secretEncrypted: encryptSecret({ plaintext: apiSecret, encryptionKey }),
      scopes,
      rateLimitPerMinute: options["rate-limit"],
      allowedIps,
      status: options.status,
      // Preserved on rotation so the record still shows when the client was last seen.
      lastUsedAt: existing?.lastUsedAt ?? null,
      createdAt: existing?.createdAt ?? timestamp,
      updatedAt: timestamp
    });

    console.log("");
    console.log(existing ? "Rotated API client credentials." : "Created API client.");
    console.log("");
    console.log(`  client id     ${clientId}`);
    console.log(`  name          ${options.name ?? clientId}`);
    console.log(`  api key       ${apiKey}`);
    console.log(`  api secret    ${apiSecret}`);
    console.log(`  scopes        ${scopes.join(", ")}`);
    console.log(`  rate limit    ${options["rate-limit"]}/min`);
    console.log(`  allowed ips   ${allowedIps.length > 0 ? allowedIps.join(", ") : "(no restriction)"}`);
    console.log(`  status        ${options.status}`);
    console.log("");
    console.log(
      generated
        ? "Key and secret were generated here and are stored encrypted. This is the only time they are shown — hand them over now, there is no way to read them back."
        : "The supplied secret is stored encrypted and cannot be read back from the database."
    );
    console.log("");
  } finally {
    await closeMongoClientSingleton({ uri: mongodbUri, appName });
  }
}

main().catch((error: unknown) => {
  console.error(`[api:create-client] ${error instanceof Error ? error.message : String(error)}`);
  console.error("");
  console.error(usage);
  process.exit(1);
});
