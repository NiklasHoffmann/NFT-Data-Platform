import type { NextRequest } from "next/server";
import { createJob, findTokensByIdentities, getMongoCollections } from "@nft-platform/db";
import { tokenIdentitySchema } from "@nft-platform/domain";
import { z } from "zod";
import {
  buildValidationErrorResponse,
  buildValidationIssues,
  safeParseJsonRequestBody
} from "../../../../../lib/api-validation";
import { withAuthenticatedRoute } from "../../../../../lib/api-auth";
import { serializeEnrichedCollections } from "../../../../../lib/collection-response";
import { getWebMongoDatabase } from "../../../../../lib/mongodb";
import { enqueueRefreshTokenJob } from "../../../../../lib/queue";
import { serializeEnrichedTokens } from "../../../../../lib/token-response";

export const dynamic = "force-dynamic";

const tokenDiscoverRequestSchema = z.object({
  items: z.array(tokenIdentitySchema).min(1).max(500)
});

const postHandler = withAuthenticatedRoute(["tokens:read", "refresh:token"], async ({ auth }) => {
  const parsedBody = safeParseJsonRequestBody(auth.bodyText);

  if (!parsedBody.ok) {
    return parsedBody.response;
  }

  const validatedPayloadResult = tokenDiscoverRequestSchema.safeParse(parsedBody.data);

  if (!validatedPayloadResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_token_discover_request",
      issues: buildValidationIssues(validatedPayloadResult.error)
    });
  }

  const validatedPayload = validatedPayloadResult.data;
  const database = getWebMongoDatabase();
  const requestedItems = dedupeTokenDiscoverItems(validatedPayload.items);
  const tokens = await findTokensByIdentities({
    database,
    identities: requestedItems
  });
  const serializedTokens = await serializeEnrichedTokens(database, tokens);
  const tokensByIdentity = new Map(
    serializedTokens.map((token) => [`${token.chainId}:${token.contractAddress}:${token.tokenId}`, token])
  );
  const collectionIdentities = [...new Map(
    tokens.map((token) => [`${token.chainId}:${token.contractAddress}`, {
      chainId: token.chainId,
      contractAddress: token.contractAddress
    }])
  ).values()];
  const collections = collectionIdentities.length > 0
    ? await getMongoCollections(database)
        .collections
        .find({
          $or: collectionIdentities
        })
        .toArray()
    : [];
  const serializedCollections = await serializeEnrichedCollections(database, collections);
  const collectionsByIdentity = new Map(
    serializedCollections.map((collection) => [`${collection.chainId}:${collection.contractAddress}`, collection])
  );
  const now = new Date();

  const responseItems = await Promise.all(requestedItems.map(async (item) => {
    const tokenIdentity = `${item.chainId}:${item.contractAddress}:${item.tokenId}`;
    const collectionIdentity = `${item.chainId}:${item.contractAddress}`;
    const token = tokensByIdentity.get(tokenIdentity) ?? null;
    const collection = collectionsByIdentity.get(collectionIdentity) ?? null;

    if (token) {
      return {
        chainId: item.chainId,
        contractAddress: item.contractAddress,
        tokenId: item.tokenId,
        discoveryStatus: "ready" as const,
        queuedJobId: null,
        jobId: null,
        token,
        collection
      };
    }

    const queuedJob = await enqueueRefreshTokenJob({
      chainId: item.chainId,
      contractAddress: item.contractAddress,
      tokenId: item.tokenId,
      forceMetadata: true,
      forceOwnership: true
    });
    const jobId = await createJob(database, {
      queueJobId: queuedJob.jobId,
      type: "refresh-token",
      payload: {
        chainId: item.chainId,
        contractAddress: item.contractAddress,
        tokenId: item.tokenId,
        forceMetadata: true,
        forceOwnership: true
      },
      status: queuedJob.status,
      attempts: queuedJob.attempts,
      lastError: queuedJob.lastError,
      createdAt: now,
      updatedAt: now
    });

    return {
      chainId: item.chainId,
      contractAddress: item.contractAddress,
      tokenId: item.tokenId,
      discoveryStatus: queuedJob.status === "failed" ? "failed" as const : "queued" as const,
      queuedJobId: queuedJob.jobId,
      jobId: jobId.toHexString(),
      token: null,
      collection: null
    };
  }));

  return Response.json({
    ok: true,
    summary: {
      total: responseItems.length,
      ready: responseItems.filter((item) => item.discoveryStatus === "ready").length,
      queued: responseItems.filter((item) => item.discoveryStatus === "queued").length,
      failed: responseItems.filter((item) => item.discoveryStatus === "failed").length
    },
    items: responseItems
  });
});

function dedupeTokenDiscoverItems(items: Array<{ chainId: number; contractAddress: string; tokenId: string }>) {
  return [...new Map(
    items.map((item) => [`${item.chainId}:${item.contractAddress.toLowerCase()}:${item.tokenId}`, item])
  ).values()];
}

export async function POST(request: NextRequest): Promise<Response> {
  return postHandler(request, undefined);
}