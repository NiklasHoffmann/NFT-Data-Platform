import { createHash, createHmac } from "node:crypto";
import process from "node:process";
import { closeAllMongoClients, findJobByQueueJobId, getMongoDatabase } from "@nft-platform/db";
import { loadLocalEnvFiles } from "@nft-platform/runtime";

loadLocalEnvFiles();

type QueuedJobResponse = {
  ok: boolean;
  queueJobId: string;
  status: string;
};

type JobStatus = {
  status: string;
  lastError: string | null;
};

const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();
const pollTimeoutMs = Number(process.env.SMOKE_REFRESH_TIMEOUT_MS ?? "45000");
const pollIntervalMs = Number(process.env.SMOKE_REFRESH_POLL_INTERVAL_MS ?? "1000");

async function main(): Promise<void> {
  const database = getMongoDatabase({
    uri: requiredEnv("MONGODB_URI"),
    databaseName: requiredEnv("MONGODB_DATABASE"),
    appName: "nft-platform-smoke-refresh"
  });

  try {
    const erc1155CollectionRefresh = await signedRequest<QueuedJobResponse>(
      "POST",
      "/api/v1/refresh/collection",
      {
        chainId: 11155111,
        contractAddress: "0x2f2a217caa0948bca6df8de110ce41720c51028e"
      }
    );
    expect(
      erc1155CollectionRefresh.status === 200 || erc1155CollectionRefresh.status === 202,
      "ERC-1155 collection refresh did not return an accepted job response."
    );

    const erc1155TokenRefresh = await signedRequest<QueuedJobResponse>(
      "POST",
      "/api/v1/refresh/token",
      {
        chainId: 11155111,
        contractAddress: "0x2f2a217caa0948bca6df8de110ce41720c51028e",
        tokenId: "4",
        forceMetadata: true,
        forceOwnership: true
      }
    );
    expect(
      erc1155TokenRefresh.status === 200 || erc1155TokenRefresh.status === 202,
      "ERC-1155 token refresh did not return an accepted job response."
    );

    const erc721CollectionRefresh = await signedRequest<QueuedJobResponse>(
      "POST",
      "/api/v1/refresh/collection",
      {
        chainId: 11155111,
        contractAddress: "0x41655ae49482de69eec8f6875c34a8ada01965e2"
      }
    );
    expect(
      erc721CollectionRefresh.status === 200 || erc721CollectionRefresh.status === 202,
      "ERC-721 collection refresh did not return an accepted job response."
    );

    const erc721TokenRefresh = await signedRequest<QueuedJobResponse>(
      "POST",
      "/api/v1/refresh/token",
      {
        chainId: 11155111,
        contractAddress: "0x41655ae49482de69eec8f6875c34a8ada01965e2",
        tokenId: "359",
        forceMetadata: true,
        forceOwnership: true
      }
    );
    expect(
      erc721TokenRefresh.status === 200 || erc721TokenRefresh.status === 202,
      "ERC-721 token refresh did not return an accepted job response."
    );

    const completedJobs = await Promise.all([
      waitForJobDone(database, erc1155CollectionRefresh.data.queueJobId),
      waitForJobDone(database, erc1155TokenRefresh.data.queueJobId),
      waitForJobDone(database, erc721CollectionRefresh.data.queueJobId),
      waitForJobDone(database, erc721TokenRefresh.data.queueJobId)
    ]);

    const erc1155Token = await signedRequest<{
      ok: boolean;
      item: { standard: string; tokenId: string; metadataStatus: string };
    }>("GET", "/api/v1/tokens/11155111/0x2f2a217caa0948bca6df8de110ce41720c51028e/4");
    expect(erc1155Token.status === 200, "ERC-1155 token read failed after refresh.");
    expect(erc1155Token.data.item.standard === "erc1155", "Expected ERC-1155 token after refresh.");

    const erc721Owners = await signedRequest<{
      ok: boolean;
      standard: string;
      items: Array<{ ownerAddress: string }>;
    }>("GET", "/api/v1/owners/11155111/0x41655ae49482de69eec8f6875c34a8ada01965e2/359?limit=10");
    expect(erc721Owners.status === 200, "ERC-721 owner read failed after refresh.");
    expect(erc721Owners.data.standard === "erc721", "Expected ERC-721 owners response after refresh.");
    expect(erc721Owners.data.items.length === 1, "Expected a single ERC-721 owner after refresh.");

    console.log(
      JSON.stringify(
        {
          ok: true,
          apiBaseUrl,
          completedJobs: completedJobs.map((job) => ({ queueJobId: job.queueJobId, status: job.status })),
          checks: [
            "refresh_collection_erc1155",
            "refresh_token_erc1155",
            "refresh_collection_erc721",
            "refresh_token_erc721",
            "post_refresh_read_erc1155",
            "post_refresh_owner_erc721"
          ]
        },
        null,
        2
      )
    );
  } finally {
    await closeAllMongoClients();
  }
}

async function waitForJobDone(database: ReturnType<typeof getMongoDatabase>, queueJobId: string) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= pollTimeoutMs) {
    const job = await findJobByQueueJobId({
      database,
      queueJobId
    });

    if (job?.status === "done") {
      return {
        queueJobId,
        status: job.status as JobStatus["status"]
      };
    }

    if (job?.status === "failed") {
      throw new Error(`Job ${queueJobId} failed: ${job.lastError ?? "unknown error"}`);
    }

    await wait(pollIntervalMs);
  }

  throw new Error(
    `Job ${queueJobId} did not complete within ${pollTimeoutMs}ms. Ensure the worker is running locally.`
  );
}

async function signedRequest<T>(method: string, path: string, body?: Record<string, unknown>) {
  const url = new URL(path, apiBaseUrl);
  const serializedBody = body ? JSON.stringify(body) : "";
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const payload = [
    method,
    `${url.pathname}${url.search}`,
    createHash("sha256").update(serializedBody).digest("hex"),
    timestamp
  ].join("\n");
  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      "x-client-id": requiredEnv("API_BOOTSTRAP_CLIENT_ID"),
      "x-api-key": requiredEnv("API_BOOTSTRAP_KEY"),
      "x-signature": createHmac("sha256", requiredEnv("API_BOOTSTRAP_SECRET")).update(payload).digest("hex"),
      "x-timestamp": timestamp
    },
    body: method === "GET" || method === "HEAD" ? undefined : serializedBody
  });
  const text = await response.text();
  const data = JSON.parse(text) as T;

  return {
    status: response.status,
    data
  };
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

function expect(condition: unknown, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

main().catch((error) => {
  console.error("[smoke:refresh] failed", error);
  process.exit(1);
});