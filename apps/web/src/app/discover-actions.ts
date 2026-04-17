"use server";

import { createJob, findCollectionByIdentity, findJobByQueueJobId, findTokenByIdentity } from "@nft-platform/db";
import { evmAddressSchema, normalizeContractAddress } from "@nft-platform/domain";
import { refreshCollectionJobSchema, refreshTokenJobSchema } from "@nft-platform/queue";
import { redirect } from "next/navigation";
import { z } from "zod";
import { getWebMongoDatabase } from "../lib/mongodb";
import { enqueueRefreshCollectionJob, enqueueRefreshTokenJob } from "../lib/queue";

type DiscoverStatus = "loaded" | "queued" | "invalid" | "not-found" | "unresolved" | "failed";

const discoverTokenFormSchema = z.object({
  chainId: z.coerce.number().int().positive(),
  contractAddress: evmAddressSchema,
  tokenId: z.string().trim().optional().transform((value) => value ?? "")
});

export async function discoverTokenAction(formData: FormData): Promise<void> {
  const parsed = discoverTokenFormSchema.safeParse({
    chainId: formData.get("chainId") ?? undefined,
    contractAddress: formData.get("contractAddress") ?? undefined,
    tokenId: formData.get("tokenId") ?? undefined
  });

  if (!parsed.success) {
    redirectToHome({
      status: "invalid",
      message: parsed.error.issues[0]?.message ?? "Invalid discover request."
    });
  }

  const database = getWebMongoDatabase();
  const timestamp = new Date();
  const normalizedContractAddress = normalizeContractAddress(parsed.data.contractAddress);
  const collectionPayload = refreshCollectionJobSchema.parse({
    chainId: parsed.data.chainId,
    contractAddress: normalizedContractAddress,
    tokenIdHint: parsed.data.tokenId || undefined
  });
  const queuedCollection = await enqueueRefreshCollectionJob(collectionPayload);

  await createJob(database, {
    queueJobId: queuedCollection.jobId,
    type: "refresh-collection",
    payload: collectionPayload,
    status: queuedCollection.status,
    attempts: queuedCollection.attempts,
    lastError: queuedCollection.lastError,
    createdAt: timestamp,
    updatedAt: timestamp
  });

  let discoverResult: { status: DiscoverStatus; message: string };

  if (parsed.data.tokenId) {
    const tokenPayload = refreshTokenJobSchema.parse({
      chainId: parsed.data.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: parsed.data.tokenId,
      forceMetadata: true,
      forceOwnership: true
    });
    const queuedToken = await enqueueRefreshTokenJob(tokenPayload);

    await createJob(database, {
      queueJobId: queuedToken.jobId,
      type: "refresh-token",
      payload: tokenPayload,
      status: queuedToken.status,
      attempts: queuedToken.attempts,
      lastError: queuedToken.lastError,
      createdAt: timestamp,
      updatedAt: timestamp
    });

    discoverResult = await waitForDiscoveryOutcome({
      database,
      tokenQueueJobId: queuedToken.jobId,
      collectionQueueJobId: queuedCollection.jobId,
      chainId: parsed.data.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: parsed.data.tokenId,
      refreshStartedAt: timestamp
    });
  } else {
    discoverResult = await waitForCollectionOutcome({
      database,
      queueJobId: queuedCollection.jobId,
      chainId: parsed.data.chainId,
      contractAddress: normalizedContractAddress,
      refreshStartedAt: timestamp
    });
  }

  redirectToHome({
    chainId: parsed.data.chainId,
    contractAddress: normalizedContractAddress,
    ...(parsed.data.tokenId ? { tokenId: parsed.data.tokenId } : {}),
    status: discoverResult.status,
    message: discoverResult.message
  });
}

async function waitForDiscoveryOutcome(params: {
  database: ReturnType<typeof getWebMongoDatabase>;
  tokenQueueJobId: string;
  collectionQueueJobId: string;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  refreshStartedAt: Date;
}): Promise<{ status: DiscoverStatus; message: string }> {
  for (let attempt = 0; attempt < 12; attempt += 1) {
    const [tokenJob, collectionJob, token, collection] = await Promise.all([
      findJobByQueueJobId({
        database: params.database,
        queueJobId: params.tokenQueueJobId
      }),
      findJobByQueueJobId({
        database: params.database,
        queueJobId: params.collectionQueueJobId
      }),
      findTokenByIdentity({
        database: params.database,
        chainId: params.chainId,
        contractAddress: params.contractAddress,
        tokenId: params.tokenId
      }),
      findCollectionByIdentity({
        database: params.database,
        chainId: params.chainId,
        contractAddress: params.contractAddress
      })
    ]);

    const collectionRefreshSettled =
      collectionJob?.status === "done" ||
      collectionJob?.status === "failed" ||
      isCollectionFreshForRequest(collection, params.refreshStartedAt);

    if (tokenJob?.status === "failed") {
      if ((tokenJob.lastError ?? "").toLowerCase().includes("not found")) {
        return {
          status: "not-found",
          message: "The collection exists, but this token could not be found on-chain. Collection-level public data can still be shown below."
        };
      }

      return {
        status: "failed",
        message: tokenJob.lastError ?? "Discovery failed while refreshing the token."
      };
    }

    if (tokenJob?.status === "done" && collectionRefreshSettled) {
      if (!token) {
        return {
          status: "not-found",
          message:
            "The collection was refreshed, but this token was not materialized from on-chain data. Collection-level public data can still be shown below."
        };
      }

      if (token.metadataStatus === "failed") {
        return {
          status: "failed",
          message: "Token was discovered, but metadata fetching failed. On-chain ownership and raw token fields may still be available below."
        };
      }

      if (isRenderableToken(token)) {
        return {
          status: "loaded",
          message:
            collectionJob?.status === "failed"
              ? "Token discovered and loaded, but the collection refresh failed. Collection data may still reflect the previous state."
              : "Token and collection discovered and loaded from the read model."
        };
      }

      return {
        status: "unresolved",
        message: "Token was discovered, but metadata is not available yet."
      };
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 750);
    });
  }

  return {
    status: "queued",
    message: "Discovery queued. Token or collection refresh is still running in the background."
  };
}

async function waitForCollectionOutcome(params: {
  database: ReturnType<typeof getWebMongoDatabase>;
  queueJobId: string;
  chainId: number;
  contractAddress: string;
  refreshStartedAt: Date;
}): Promise<{ status: DiscoverStatus; message: string }> {
  for (let attempt = 0; attempt < 12; attempt += 1) {
    const [job, collection] = await Promise.all([
      findJobByQueueJobId({
        database: params.database,
        queueJobId: params.queueJobId
      }),
      findCollectionByIdentity({
        database: params.database,
        chainId: params.chainId,
        contractAddress: params.contractAddress
      })
    ]);

    if (job?.status === "failed") {
      return {
        status: "failed",
        message: job.lastError ?? "Discovery failed while refreshing the collection."
      };
    }

    if ((job?.status === "done" || isCollectionFreshForRequest(collection, params.refreshStartedAt)) && collection) {
      return {
        status: "loaded",
        message: "Collection discovered and loaded from the read model."
      };
    }

    await new Promise((resolve) => {
      setTimeout(resolve, 750);
    });
  }

  return {
    status: "queued",
    message: "Collection discovery queued. Refresh is still running in the background."
  };
}

function isRenderableToken(token: NonNullable<Awaited<ReturnType<typeof findTokenByIdentity>>>): boolean {
  return Boolean(
    token.name ||
      token.description ||
      token.metadataUriResolved ||
      token.metadataUriRaw ||
      token.imageOriginalUrl ||
      token.animationOriginalUrl ||
      token.audioOriginalUrl ||
      token.interactiveOriginalUrl ||
      token.imageAssetId ||
      token.animationAssetId ||
      token.audioAssetId ||
      token.metadataStatus === "ok"
  );
}

function isCollectionFreshForRequest(
  collection: NonNullable<Awaited<ReturnType<typeof findCollectionByIdentity>>> | null,
  refreshStartedAt: Date
): boolean {
  if (!collection) {
    return false;
  }

  return collection.updatedAt.getTime() >= refreshStartedAt.getTime();
}

function redirectToHome(params: {
  chainId?: number;
  contractAddress?: string;
  tokenId?: string;
  status: DiscoverStatus;
  message: string;
}): never {
  const searchParams = new URLSearchParams();

  if (params.chainId !== undefined) {
    searchParams.set("chainId", String(params.chainId));
  }

  if (params.contractAddress) {
    searchParams.set("contractAddress", params.contractAddress);
  }

  if (params.tokenId) {
    searchParams.set("tokenId", params.tokenId);
  }

  searchParams.set("status", params.status);
  searchParams.set("message", params.message);

  redirect(`/?${searchParams.toString()}`);
}