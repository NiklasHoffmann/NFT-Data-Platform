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

type TokenReadResponse = {
  ok: boolean;
  item: {
    standard: string;
    tokenId: string;
    metadataStatus: string;
    mediaStatus: string;
    supplyQuantity: string | null;
    interactiveOriginalUrl: string | null;
    interactiveMediaType: string | null;
    media: {
      image: {
        mimeType: string | null;
      } | null;
    };
  };
};

type RegressionCase = {
  name: string;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  expectJobStatus: "done" | "failed";
  expectedErrorIncludes?: string;
  validateToken?: (token: TokenReadResponse["item"]) => void;
};

const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();
const pollTimeoutMs = Number(process.env.SMOKE_REFRESH_TIMEOUT_MS ?? "45000");
const pollIntervalMs = Number(process.env.SMOKE_REFRESH_POLL_INTERVAL_MS ?? "1000");

const regressionCases: RegressionCase[] = [
  {
    name: "erc1155_quantity_blockbar_1005",
    chainId: 11155111,
    contractAddress: "0x94a3e770bea98c65293653d084df9410230bd206",
    tokenId: "1005",
    expectJobStatus: "done",
    validateToken: (token) => {
      expect(token.standard === "erc1155", "Expected ERC-1155 standard for BlockBar 1005.");
      expect(token.supplyQuantity === "42", "Expected supplyQuantity=42 for BlockBar 1005.");
    }
  },
  {
    name: "svg_media_blockbar_1010",
    chainId: 11155111,
    contractAddress: "0x94a3e770bea98c65293653d084df9410230bd206",
    tokenId: "1010",
    expectJobStatus: "done",
    validateToken: (token) => {
      expect(token.media.image?.mimeType === "image/svg+xml", "Expected SVG image mime type for BlockBar 1010.");
    }
  },
  {
    name: "metadata_failure_tolerance_d426_token_1",
    chainId: 11155111,
    contractAddress: "0xd4261f9e2385c927703d305d015813c640c6f027",
    tokenId: "1",
    expectJobStatus: "done",
    validateToken: (token) => {
      expect(token.tokenId === "1", "Expected token 1 to materialize for d426 regression case.");
    }
  },
  {
    name: "no_bytecode_e8df_token_9000001",
    chainId: 11155111,
    contractAddress: "0xe8df60a93b2b328397a8cbf73f0d732aaa11e33d",
    tokenId: "9000001",
    expectJobStatus: "failed",
    expectedErrorIncludes: "no deployed contract bytecode"
  },
  {
    name: "partial_media_myfi_251",
    chainId: 1,
    contractAddress: "0x207e8b8167890ce966d8486b2954114a269a40cc",
    tokenId: "251",
    expectJobStatus: "done",
    validateToken: (token) => {
      expect(token.mediaStatus === "partial", "Expected partial media status for MYFI token 251.");
      expect(token.media.image?.mimeType === "image/png", "Expected local image media for MYFI token 251.");
    }
  }
];

async function main(): Promise<void> {
  const database = getMongoDatabase({
    uri: requiredEnv("MONGODB_URI"),
    databaseName: requiredEnv("MONGODB_DATABASE"),
    appName: "nft-platform-smoke-discover-regressions"
  });

  try {
    const results = [] as Array<Record<string, unknown>>;

    for (const regressionCase of regressionCases) {
      const queuedTokenRefresh = await signedRequest<QueuedJobResponse>("POST", "/api/v1/refresh/token", {
        chainId: regressionCase.chainId,
        contractAddress: regressionCase.contractAddress,
        tokenId: regressionCase.tokenId,
        forceMetadata: true,
        forceOwnership: true
      });

      expect(
        queuedTokenRefresh.status === 200 || queuedTokenRefresh.status === 202,
        `Refresh request failed for ${regressionCase.name}.`
      );

      const completedJob = await waitForJobTerminal(database, queuedTokenRefresh.data.queueJobId);

      expect(
        completedJob.status === regressionCase.expectJobStatus,
        `Expected job status ${regressionCase.expectJobStatus} for ${regressionCase.name}, got ${completedJob.status}.`
      );

      if (regressionCase.expectJobStatus === "failed") {
        expect(
          (completedJob.lastError ?? "").toLowerCase().includes((regressionCase.expectedErrorIncludes ?? "").toLowerCase()),
          `Unexpected error for ${regressionCase.name}: ${completedJob.lastError ?? "no error message"}`
        );

        results.push({
          name: regressionCase.name,
          queueJobId: queuedTokenRefresh.data.queueJobId,
          status: completedJob.status,
          lastError: completedJob.lastError
        });
        continue;
      }

      const tokenResponse = await waitForTokenValidation(regressionCase);

      results.push({
        name: regressionCase.name,
        queueJobId: queuedTokenRefresh.data.queueJobId,
        status: completedJob.status,
        metadataStatus: tokenResponse.data.item.metadataStatus,
        mediaStatus: tokenResponse.data.item.mediaStatus,
        supplyQuantity: tokenResponse.data.item.supplyQuantity,
        interactiveMediaType: tokenResponse.data.item.interactiveMediaType
      });
    }

    console.log(
      JSON.stringify(
        {
          ok: true,
          apiBaseUrl,
          cases: results
        },
        null,
        2
      )
    );
  } finally {
    await closeAllMongoClients();
  }
}

async function waitForTokenValidation(regressionCase: RegressionCase) {
  const startedAt = Date.now();
  let lastError: Error | null = null;

  while (Date.now() - startedAt <= pollTimeoutMs) {
    const tokenResponse = await signedRequest<TokenReadResponse>(
      "GET",
      `/api/v1/tokens/${regressionCase.chainId}/${regressionCase.contractAddress}/${regressionCase.tokenId}`
    );

    expect(tokenResponse.status === 200, `Token read failed for ${regressionCase.name}.`);

    try {
      regressionCase.validateToken?.(tokenResponse.data.item);
      return tokenResponse;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      await wait(pollIntervalMs);
    }
  }

  throw lastError ?? new Error(`Token validation timed out for ${regressionCase.name}.`);
}

async function waitForJobTerminal(database: ReturnType<typeof getMongoDatabase>, queueJobId: string) {
  const startedAt = Date.now();

  while (Date.now() - startedAt <= pollTimeoutMs) {
    const job = await findJobByQueueJobId({
      database,
      queueJobId
    });

    if (job?.status === "done" || job?.status === "failed") {
      return {
        queueJobId,
        status: job.status,
        lastError: job.lastError
      };
    }

    await wait(pollIntervalMs);
  }

  throw new Error(`Job ${queueJobId} did not complete within ${pollTimeoutMs}ms. Ensure the worker is running locally.`);
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
  console.error("[smoke:discover-regressions] failed", error);
  process.exit(1);
});