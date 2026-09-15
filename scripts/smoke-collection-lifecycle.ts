import { createHash, createHmac } from "node:crypto";
import process from "node:process";
import { closeAllMongoClients, findCollectionByIdentity, getMongoDatabase } from "@nft-platform/db";
import { loadLocalEnvFiles } from "@nft-platform/runtime";

loadLocalEnvFiles();

/**
 * Verifies that disabling a collection actually stops work on it, and that re-enabling resumes from
 * the stored checkpoint.
 *
 * The part worth checking is not the endpoint but the overwrite protection behind it: refresh and
 * reindex both write `syncStatus: "active"` unconditionally, so a disabled collection that is
 * refreshed once must still come back disabled.
 */
const apiBaseUrl = (process.env.API_BASE_URL ?? "http://localhost:3000").trim();
const chainId = Number(process.env.SMOKE_LIFECYCLE_CHAIN_ID ?? "11155111");
const contractAddress = (
  process.env.SMOKE_LIFECYCLE_CONTRACT ?? "0x2f2a217caa0948bca6df8de110ce41720c51028e"
).trim();

type StatusResponse = {
  ok: boolean;
  previousSyncStatus: string;
  syncStatus: string;
  lastIndexedBlock: number | null;
};

async function main(): Promise<void> {
  const database = getMongoDatabase({
    uri: requiredEnv("MONGODB_URI"),
    databaseName: requiredEnv("MONGODB_DATABASE"),
    appName: "nft-platform-smoke-collection-lifecycle"
  });

  try {
    const registration = await signedRequest("POST", "/api/v1/refresh/collection", {
      chainId,
      contractAddress
    });
    expect(
      registration.status === 200 || registration.status === 202,
      "Collection registration did not return an accepted job response."
    );

    const checkpointBeforeDisable = (
      await findCollectionByIdentity({ database, chainId, contractAddress })
    )?.lastIndexedBlock ?? null;

    const disabled = await signedRequest<StatusResponse>("POST", statusPath(), {
      syncStatus: "disabled"
    });
    expect(disabled.status === 200, `Disabling the collection returned ${disabled.status}.`);
    expect(disabled.data.syncStatus === "disabled", "The collection was not reported as disabled.");

    const refreshWhileDisabled = await signedRequest("POST", "/api/v1/refresh/collection", {
      chainId,
      contractAddress
    });
    expect(
      refreshWhileDisabled.status === 409,
      `Refreshing a disabled collection returned ${refreshWhileDisabled.status} instead of 409.`
    );

    const reindexWhileDisabled = await signedRequest("POST", "/api/v1/reindex", {
      chainId,
      contractAddress,
      fromBlock: 0,
      toBlock: 1
    });
    expect(
      reindexWhileDisabled.status === 409,
      `Reindexing a disabled collection returned ${reindexWhileDisabled.status} instead of 409.`
    );

    const storedWhileDisabled = await findCollectionByIdentity({ database, chainId, contractAddress });
    expect(
      storedWhileDisabled?.syncStatus === "disabled",
      "The stored collection left the disabled state while it should have been switched off."
    );

    const reactivated = await signedRequest<StatusResponse>("POST", statusPath(), {
      syncStatus: "active"
    });
    expect(reactivated.status === 200, `Re-enabling the collection returned ${reactivated.status}.`);
    expect(reactivated.data.syncStatus === "active", "The collection was not reported as active again.");
    expect(
      reactivated.data.lastIndexedBlock === checkpointBeforeDisable,
      "Re-enabling moved the indexing checkpoint; reactivation must resume, not start over."
    );

    console.log(
      JSON.stringify(
        {
          ok: true,
          apiBaseUrl,
          chainId,
          contractAddress,
          checkpointBeforeDisable,
          checks: [
            "disable_sets_status",
            "refresh_rejected_while_disabled",
            "reindex_rejected_while_disabled",
            "status_survives_refresh_attempt",
            "reactivate_keeps_checkpoint"
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

function statusPath(): string {
  return `/api/v1/collections/${chainId}/${contractAddress}/status`;
}

async function signedRequest<T = Record<string, unknown>>(
  method: string,
  path: string,
  body?: Record<string, unknown>
) {
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

  return {
    status: response.status,
    data: JSON.parse(text) as T
  };
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
  console.error("[smoke:collection-lifecycle] failed", error);
  process.exit(1);
});
