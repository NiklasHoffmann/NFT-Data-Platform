import { createHash, createHmac } from "node:crypto";
import process from "node:process";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { z } from "zod";

// loadLocalEnvFiles applies .env with override, which would replace the values an
// operator exports in their shell. This script usually runs against a deployed API
// while .env holds the local stack, so an explicit shell value has to win: snapshot
// what was already set, load the files for everything else, then restore.
const explicitEnv = new Map(
  ["API_BASE_URL", "API_BOOTSTRAP_CLIENT_ID", "API_BOOTSTRAP_KEY", "API_BOOTSTRAP_SECRET"]
    .map((name): [string, string | undefined] => [name, process.env[name]])
    .filter((entry): entry is [string, string] => entry[1] !== undefined)
);

loadLocalEnvFiles();

for (const [name, value] of explicitEnv) {
  process.env[name] = value;
}

const booleanFlags = new Set(["dry-run", "help"]);

const optionsSchema = z
  .object({
    help: z.boolean().default(false),
    "dry-run": z.boolean().default(false),
    "chain-id": z.coerce.number().int().positive().optional(),
    contract: z
      .string()
      .trim()
      .regex(/^0x[a-fA-F0-9]{40}$/u, "Contract address must be a 0x-prefixed 20 byte address.")
      .optional(),
    limit: z.coerce.number().int().positive().optional(),
    "page-size": z.coerce.number().int().positive().max(200).default(100),
    "delay-ms": z.coerce.number().int().nonnegative().default(300)
  })
  .strict();

const usage = `
Usage: npm run media:remirror -- [options]

Re-mirrors every token's media into the currently configured S3 bucket by queueing
one refresh-media job per token with forceDownload set. Use it after pointing
S3_ENDPOINT and S3_PUBLIC_BASE_URL at a different object store: the worker
re-downloads each source, uploads it to the new bucket, and overwrites the stored
cdnUrl fields, which otherwise keep pointing at the old host.

Reads token pages from GET /api/v1/tokens and posts to POST /api/v1/refresh/media,
so it talks to the deployed API over HTTPS and needs no direct Redis or Mongo access.
The client needs the tokens:read and refresh:media scopes.

Options
  --chain-id <id>         Restrict to one chain.
  --contract <address>    Restrict to one contract. Combine with --chain-id.
  --limit <n>             Stop after queueing n tokens. Default: no limit.
  --page-size <n>         Tokens per listing request, 1-200. Default 100.
  --delay-ms <ms>         Pause between refresh requests. Default 300, which stays
                          under a 300 requests per minute client rate limit.
  --dry-run               List what would be queued without posting anything.

Environment
  API_BASE_URL            Target API. Default http://localhost:3000.
  API_BOOTSTRAP_CLIENT_ID, API_BOOTSTRAP_KEY, API_BOOTSTRAP_SECRET

Jobs are idempotent per token, so an interrupted run can simply be repeated.
`.trim();

const tokenListResponseSchema = z.object({
  ok: z.literal(true),
  items: z.array(
    z.object({
      chainId: z.number().int().positive(),
      contractAddress: z.string(),
      tokenId: z.string(),
      mediaStatus: z.string(),
      imageOriginalUrl: z.string().nullable(),
      animationOriginalUrl: z.string().nullable(),
      audioOriginalUrl: z.string().nullable()
    })
  ),
  pageInfo: z.object({
    hasMore: z.boolean(),
    nextCursor: z.string().nullable()
  })
});

const refreshResponseSchema = z.object({
  ok: z.literal(true),
  queueJobId: z.string(),
  status: z.string()
});

type TokenListItem = z.infer<typeof tokenListResponseSchema>["items"][number];

type ParsedArguments = Record<string, string | boolean>;

type ApiResponse = {
  status: number;
  retryAfterSeconds: number | null;
  body: unknown;
};

async function main(): Promise<void> {
  const options = optionsSchema.parse(parseArguments(process.argv.slice(2)));

  if (options.help) {
    console.log(usage);
    return;
  }

  if (options.contract && options["chain-id"] === undefined) {
    throw new Error("--contract requires --chain-id, because a contract address is only unique per chain.");
  }

  const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();
  const queued: string[] = [];
  const skipped: string[] = [];
  const failed: Array<{ token: string; status: number; body: unknown }> = [];
  let cursor: string | null = null;
  let hasMorePages = true;
  let inspected = 0;

  // Naming both makes a run against the wrong stack visible in the first line,
  // rather than as a confusing 401 further down.
  console.log(
    `[media:remirror] target ${apiBaseUrl} as ${requiredEnv("API_BOOTSTRAP_CLIENT_ID")}` +
      `${options["dry-run"] ? " (dry run, nothing is queued)" : ""}`
  );

  while (hasMorePages) {
    const page = await fetchTokenPage({
      apiBaseUrl,
      cursor,
      pageSize: options["page-size"],
      chainId: options["chain-id"],
      contractAddress: options.contract
    });

    cursor = page.pageInfo.hasMore ? page.pageInfo.nextCursor : null;
    hasMorePages = cursor !== null;

    for (const token of page.items) {
      if (options.limit !== undefined && queued.length >= options.limit) {
        hasMorePages = false;
        break;
      }

      inspected += 1;
      const label = describeToken(token);

      if (!hasMediaSource(token)) {
        skipped.push(label);
        continue;
      }

      if (options["dry-run"]) {
        queued.push(label);
        console.log(`[media:remirror] would queue ${label}`);
        continue;
      }

      const result = await requestMediaRefresh({ apiBaseUrl, token });

      if (result.ok) {
        queued.push(label);
        console.log(`[media:remirror] queued ${label} -> ${result.queueJobId} (${result.status})`);
      } else {
        failed.push({ token: label, status: result.status, body: result.body });
        console.error(`[media:remirror] failed ${label} -> HTTP ${result.status}`);
      }

      if (options["delay-ms"] > 0) {
        await wait(options["delay-ms"]);
      }
    }
  }

  console.log(
    JSON.stringify(
      {
        ok: failed.length === 0,
        apiBaseUrl,
        dryRun: options["dry-run"],
        inspected,
        queued: queued.length,
        skippedWithoutMediaSource: skipped.length,
        failed
      },
      null,
      2
    )
  );

  if (failed.length > 0) {
    process.exit(1);
  }
}

async function fetchTokenPage(params: {
  apiBaseUrl: string;
  cursor: string | null;
  pageSize: number;
  chainId: number | undefined;
  contractAddress: string | undefined;
}): Promise<z.infer<typeof tokenListResponseSchema>> {
  const search = new URLSearchParams({ limit: String(params.pageSize) });

  if (params.cursor) {
    search.set("cursor", params.cursor);
  }

  if (params.chainId !== undefined) {
    search.set("chainId", String(params.chainId));
  }

  if (params.contractAddress) {
    search.set("contractAddress", params.contractAddress);
  }

  const response = await signedRequest({
    apiBaseUrl: params.apiBaseUrl,
    method: "GET",
    path: `/api/v1/tokens?${search.toString()}`
  });

  if (response.status !== 200) {
    throw new Error(
      `Token listing failed with HTTP ${response.status}: ${JSON.stringify(response.body)}`
    );
  }

  return tokenListResponseSchema.parse(response.body);
}

async function requestMediaRefresh(params: {
  apiBaseUrl: string;
  token: TokenListItem;
}): Promise<{ ok: true; queueJobId: string; status: string } | { ok: false; status: number; body: unknown }> {
  const response = await signedRequest({
    apiBaseUrl: params.apiBaseUrl,
    method: "POST",
    path: "/api/v1/refresh/media",
    body: {
      chainId: params.token.chainId,
      contractAddress: params.token.contractAddress,
      tokenId: params.token.tokenId,
      forceDownload: true
    }
  });

  if (response.status !== 200 && response.status !== 202) {
    return {
      ok: false,
      status: response.status,
      body: response.body
    };
  }

  const parsed = refreshResponseSchema.safeParse(response.body);

  if (!parsed.success) {
    return {
      ok: false,
      status: response.status,
      body: response.body
    };
  }

  return {
    ok: true,
    queueJobId: parsed.data.queueJobId,
    status: parsed.data.status
  };
}

async function signedRequest(params: {
  apiBaseUrl: string;
  method: string;
  path: string;
  body?: Record<string, unknown>;
}): Promise<ApiResponse> {
  let attempt = 0;

  for (;;) {
    const response = await sendSignedRequest(params);

    if (response.status !== 429 || attempt >= 5) {
      return response;
    }

    attempt += 1;
    const backoffMs = (response.retryAfterSeconds ?? attempt * 2) * 1000;
    console.warn(`[media:remirror] rate limited, waiting ${backoffMs}ms before retry ${attempt}`);
    await wait(backoffMs);
  }
}

async function sendSignedRequest(params: {
  apiBaseUrl: string;
  method: string;
  path: string;
  body?: Record<string, unknown>;
}): Promise<ApiResponse> {
  const url = new URL(params.path, params.apiBaseUrl);
  const serializedBody = params.body ? JSON.stringify(params.body) : "";
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const signaturePayload = [
    params.method,
    `${url.pathname}${url.search}`,
    createHash("sha256").update(serializedBody).digest("hex"),
    timestamp
  ].join("\n");

  const response = await fetch(url, {
    method: params.method,
    headers: {
      "content-type": "application/json",
      "x-client-id": requiredEnv("API_BOOTSTRAP_CLIENT_ID"),
      "x-api-key": requiredEnv("API_BOOTSTRAP_KEY"),
      "x-signature": createHmac("sha256", requiredEnv("API_BOOTSTRAP_SECRET"))
        .update(signaturePayload)
        .digest("hex"),
      "x-timestamp": timestamp
    },
    body: params.method === "GET" || params.method === "HEAD" ? undefined : serializedBody
  });

  const text = await response.text();
  const retryAfterHeader = response.headers.get("retry-after");
  const retryAfterSeconds = retryAfterHeader ? Number(retryAfterHeader) : null;

  return {
    status: response.status,
    retryAfterSeconds:
      retryAfterSeconds !== null && Number.isFinite(retryAfterSeconds) ? retryAfterSeconds : null,
    body: parseJsonOrText(text)
  };
}

function parseJsonOrText(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function hasMediaSource(token: TokenListItem): boolean {
  return Boolean(token.imageOriginalUrl || token.animationOriginalUrl || token.audioOriginalUrl);
}

function describeToken(token: TokenListItem): string {
  return `${token.chainId}/${token.contractAddress}/${token.tokenId}`;
}

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

    const next = argv[index + 1];

    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Missing value for --${withoutDashes}`);
    }

    parsed[withoutDashes] = next;
    index += 1;
  }

  return parsed;
}

function wait(timeoutMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, timeoutMs);
  });
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

main().catch((error) => {
  console.error("[media:remirror] failed", error);
  process.exit(1);
});
