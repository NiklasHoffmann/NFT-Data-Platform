import { createHash, createHmac } from "node:crypto";
import process from "node:process";
import IORedis from "ioredis";
import { createChainPublicClient, readErc721TransfersInRange } from "@nft-platform/chain";
import { closeAllMongoClients, findJobByQueueJobId, getMongoDatabase } from "@nft-platform/db";
import { buildIdempotencyKey, queueNames } from "@nft-platform/queue";
import { loadLocalEnvFiles } from "@nft-platform/runtime";
import { getWorkerRuntimeConfig } from "../apps/worker/src/env";
import { processQueueJob } from "../apps/worker/src/jobs/processors";

loadLocalEnvFiles();

type CollectionResponse = {
  ok: boolean;
  item: {
    contractAddress: string;
    deployBlock: number | null;
    lastObservedBlock: number | null;
    lastIndexedBlock: number | null;
    standard: string;
  };
};

type OwnerResponse = {
  ok: boolean;
  standard: string;
  items: Array<{
    ownerAddress: string;
  }>;
};

const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();
const pollTimeoutMs = Number(process.env.SMOKE_REFRESH_TIMEOUT_MS ?? "45000");
const pollIntervalMs = Number(process.env.SMOKE_REFRESH_POLL_INTERVAL_MS ?? "1000");
const fixture = {
  chainId: 1,
  contractAddress: "0x23ae7a05f598fc234ee9dbef04033080dea8ab19",
  tokenId: "13456321072659166509",
  replayToBlock: 23993118
} as const;

async function main(): Promise<void> {
  const workerConfig = getWorkerRuntimeConfig();
  const database = getMongoDatabase({
    uri: requiredEnv("MONGODB_URI"),
    databaseName: requiredEnv("MONGODB_DATABASE"),
    appName: "nft-platform-smoke-reindex-erc721"
  });
  const redisConnection = new IORedis(workerConfig.redisUrl, {
    maxRetriesPerRequest: null
  });
  const publicClient = createChainPublicClient({
    chainId: fixture.chainId,
    rpcUrl: workerConfig.rpcMainnetUrl
  });

  try {
    await runProcessorJob({
      queueName: queueNames.refreshCollection,
      data: {
        chainId: fixture.chainId,
        contractAddress: fixture.contractAddress,
        fullRescan: false
      },
      database,
      redisConnection,
      workerConfig
    });

    await runProcessorJob({
      queueName: queueNames.refreshToken,
      data: {
        chainId: fixture.chainId,
        contractAddress: fixture.contractAddress,
        tokenId: fixture.tokenId,
        forceMetadata: false,
        forceOwnership: true
      },
      database,
      redisConnection,
      workerConfig
    });

    const collectionResponse = await signedRequest<CollectionResponse>(
      "GET",
      `/api/v1/collections/${fixture.chainId}/${fixture.contractAddress}`
    );

    expect(collectionResponse.status === 200, "ERC-721 collection read failed before reindex test.");
    expect(collectionResponse.data.item.standard === "erc721", "Expected ERC-721 collection before reindex test.");

    const transfers = await readErc721TransfersInRange({
      client: publicClient,
      contractAddress: fixture.contractAddress,
      fromBlock: collectionResponse.data.item.deployBlock ?? fixture.replayToBlock,
      toBlock: fixture.replayToBlock
    });
    const expectedOwnerAddress = computeExpectedOwnerAddress({
      transfers,
      tokenId: fixture.tokenId
    });

    expect(Boolean(expectedOwnerAddress), "Expected bounded ERC-721 replay history to produce an owner.");

    const deletedOwnerships = await database.collection("erc721_ownership").deleteMany({
      chainId: fixture.chainId,
      contractAddress: fixture.contractAddress
    });

    expect(deletedOwnerships.deletedCount > 0, "Expected existing ERC-721 ownership rows to delete before reindex.");

    const afterDelete = await signedRequest<OwnerResponse>(
      "GET",
      `/api/v1/owners/${fixture.chainId}/${fixture.contractAddress}/${fixture.tokenId}?limit=10`
    );

    expect(afterDelete.status === 200, "ERC-721 owner read failed after deleting snapshot.");
    expect(afterDelete.data.items.length === 0, "Expected no ERC-721 owner rows after deleting the snapshot.");

    const reindexJob = await runProcessorJob({
      queueName: queueNames.reindexRange,
      data: {
        chainId: fixture.chainId,
        contractAddress: fixture.contractAddress,
        fromBlock: fixture.replayToBlock,
        toBlock: fixture.replayToBlock
      },
      database,
      redisConnection,
      workerConfig
    });

    const rebuiltOwner = await waitForOwner(expectedOwnerAddress);

    const refreshedCollection = await signedRequest<CollectionResponse>(
      "GET",
      `/api/v1/collections/${fixture.chainId}/${fixture.contractAddress}`
    );

    expect(refreshedCollection.status === 200, "ERC-721 collection read failed after reindex.");
    expect(
      refreshedCollection.data.item.lastIndexedBlock !== null &&
        refreshedCollection.data.item.lastIndexedBlock >= fixture.replayToBlock,
      "Expected lastIndexedBlock to advance through the reindex target block."
    );

    console.log(
      JSON.stringify(
        {
          ok: true,
          apiBaseUrl,
          fixture,
          queueJobId: reindexJob.queueJobId,
          restoredOwnerAddress: rebuiltOwner.data.items[0]?.ownerAddress ?? null,
          expectedOwnerAddress,
          deletedOwnershipRows: deletedOwnerships.deletedCount,
          reindexBlock: fixture.replayToBlock
        },
        null,
        2
      )
    );
  } finally {
    await redisConnection.quit();
    await closeAllMongoClients();
  }
}

function computeExpectedOwnerAddress(params: {
  transfers: Array<{
    fromAddress: string | null;
    toAddress: string | null;
    tokenId: string;
  }>;
  tokenId: string;
}): string | null {
  let ownerAddress: string | null = null;

  for (const transfer of params.transfers) {
    if (transfer.tokenId !== params.tokenId) {
      continue;
    }

    ownerAddress = transfer.toAddress;
  }

  return ownerAddress;
}

async function runProcessorJob(params: {
  queueName: (typeof queueNames)[keyof typeof queueNames];
  data: Record<string, unknown>;
  database: ReturnType<typeof getMongoDatabase>;
  redisConnection: IORedis;
  workerConfig: ReturnType<typeof getWorkerRuntimeConfig>;
}) {
  const jobId = buildIdempotencyKey(params.queueName, params.data);

  await processQueueJob({
    queueName: params.queueName,
    job: {
      id: jobId,
      data: params.data,
      attemptsMade: 0,
      opts: { attempts: 1 }
    } as never,
    context: {
      database: params.database,
      redisConnection: params.redisConnection,
      rpcMainnetUrl: params.workerConfig.rpcMainnetUrl,
      rpcSepoliaUrl: params.workerConfig.rpcSepoliaUrl,
      storage: params.workerConfig.storage,
      mediaMaxVideoBytes: params.workerConfig.mediaMaxVideoBytes
    }
  });

  const persistedJob = await findJobByQueueJobId({
    database: params.database,
    queueJobId: jobId
  });

  expect(persistedJob?.status === "done", `Expected ${params.queueName} processor job to finish with status done.`);

  return {
    queueJobId: jobId,
    status: persistedJob.status,
    lastError: persistedJob.lastError
  };
}

async function waitForOwner(expectedOwnerAddress: string) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= pollTimeoutMs) {
    const ownerResponse = await signedRequest<OwnerResponse>(
      "GET",
      `/api/v1/owners/${fixture.chainId}/${fixture.contractAddress}/${fixture.tokenId}?limit=10`
    );

    expect(ownerResponse.status === 200, "ERC-721 owner read failed while waiting for reindex rebuild.");

    if (ownerResponse.data.items[0]?.ownerAddress === expectedOwnerAddress) {
      return ownerResponse;
    }

    await wait(pollIntervalMs);
  }

  throw new Error(`ERC-721 owner was not rebuilt within ${pollTimeoutMs}ms.`);
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
  console.error("[smoke:reindex-erc721] failed", error);
  process.exit(1);
});