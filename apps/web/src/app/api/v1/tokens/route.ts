import type { NextRequest } from "next/server";
import { mediaStatusSchema, metadataStatusSchema } from "@nft-platform/domain";
import { listTokens } from "@nft-platform/db";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { buildValidationErrorResponse, buildValidationIssues, safeDecodeUpdatedAtCursor } from "../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { decodeUpdatedAtCursor, encodeUpdatedAtCursor } from "../../../../lib/cursor-pagination";
import { serializeEnrichedTokens } from "../../../../lib/token-response";

export const dynamic = "force-dynamic";

const tokenListQuerySchema = z.object({
  chainId: z.coerce.number().int().positive().optional(),
  contractAddress: z.string().trim().regex(/^0x[a-fA-F0-9]{40}$/).optional(),
  metadataStatus: metadataStatusSchema.optional(),
  mediaStatus: mediaStatusSchema.optional(),
  traitType: z.string().trim().min(1).optional(),
  traitValue: z.string().trim().min(1).optional(),
  cursor: z.string().trim().min(1).optional(),
  limit: z.coerce.number().int().positive().max(200).default(50)
});

const getHandler = withAuthenticatedRoute(["tokens:read"], async ({ request }) => {
  const parsedQueryResult = tokenListQuerySchema.safeParse({
    chainId: request.nextUrl.searchParams.get("chainId") ?? undefined,
    contractAddress: request.nextUrl.searchParams.get("contractAddress") ?? undefined,
    metadataStatus: request.nextUrl.searchParams.get("metadataStatus") ?? undefined,
    mediaStatus: request.nextUrl.searchParams.get("mediaStatus") ?? undefined,
    traitType: request.nextUrl.searchParams.get("traitType") ?? undefined,
    traitValue: request.nextUrl.searchParams.get("traitValue") ?? undefined,
    cursor: request.nextUrl.searchParams.get("cursor") ?? undefined,
    limit: request.nextUrl.searchParams.get("limit") ?? undefined
  });

  if (!parsedQueryResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_token_list_query",
      issues: buildValidationIssues(parsedQueryResult.error)
    });
  }

  const parsedQuery = parsedQueryResult.data;
  const database = getWebMongoDatabase();
  const tokenListParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    limit: number;
    chainId?: number;
    contractAddress?: string;
    metadataStatus?: z.infer<typeof metadataStatusSchema>;
    mediaStatus?: z.infer<typeof mediaStatusSchema>;
    traitType?: string;
    traitValue?: string | number | boolean;
    cursor?: {
      updatedAt: Date;
      id: ObjectId;
    };
  } = {
    database,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.chainId !== undefined) {
    tokenListParams.chainId = parsedQuery.chainId;
  }

  if (parsedQuery.contractAddress) {
    tokenListParams.contractAddress = parsedQuery.contractAddress;
  }

  if (parsedQuery.metadataStatus) {
    tokenListParams.metadataStatus = parsedQuery.metadataStatus;
  }

  if (parsedQuery.mediaStatus) {
    tokenListParams.mediaStatus = parsedQuery.mediaStatus;
  }

  if (parsedQuery.traitType) {
    tokenListParams.traitType = parsedQuery.traitType;
  }

  const parsedTraitValue = parseTraitValue(parsedQuery.traitValue);

  if (parsedTraitValue !== undefined) {
    tokenListParams.traitValue = parsedTraitValue;
  }

  if (parsedQuery.cursor) {
    const cursorResult = safeDecodeUpdatedAtCursor(parsedQuery.cursor);

    if (!cursorResult.ok) {
      return cursorResult.response;
    }

    tokenListParams.cursor = cursorResult.value;
  }

  const tokens = await listTokens(tokenListParams);
  const hasMore = tokens.length > parsedQuery.limit;
  const pageTokens = hasMore ? tokens.slice(0, parsedQuery.limit) : tokens;
  const lastPageToken = pageTokens.at(-1);
  const nextCursor = hasMore && lastPageToken ? encodeUpdatedAtCursor(lastPageToken) : null;

  return Response.json({
    ok: true,
    items: await serializeEnrichedTokens(database, pageTokens),
    pageInfo: {
      limit: parsedQuery.limit,
      hasMore,
      nextCursor
    }
  });
});

function parseTraitValue(value: string | undefined): string | number | boolean | undefined {
  if (!value) {
    return undefined;
  }

  const normalized = value.trim();

  if (/^(true|false)$/i.test(normalized)) {
    return normalized.toLowerCase() === "true";
  }

  if (/^-?\d+(\.\d+)?$/u.test(normalized)) {
    return Number(normalized);
  }

  return normalized;
}

export async function GET(request: NextRequest): Promise<Response> {
  return getHandler(request, undefined);
}
