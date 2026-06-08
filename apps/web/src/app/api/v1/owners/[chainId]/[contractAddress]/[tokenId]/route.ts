import type { NextRequest } from "next/server";
import {
  findErc721OwnershipByToken,
  findTokenByIdentity,
  listErc1155Balances,
  serializeErc721OwnershipDocument,
  serializeErc1155BalanceDocument
} from "@nft-platform/db";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeDecodeUpdatedAtCursor } from "../../../../../../../lib/api-validation";
import { withAuthenticatedRoute } from "../../../../../../../lib/api-auth";
import { decodeUpdatedAtCursor, encodeUpdatedAtCursor } from "../../../../../../../lib/cursor-pagination";
import { getWebMongoDatabase } from "../../../../../../../lib/mongodb";

export const dynamic = "force-dynamic";

const holderListQuerySchema = z.object({
  cursor: z.string().trim().min(1).optional(),
  limit: z.coerce.number().int().positive().max(200).default(50)
});

type TokenOwnersRouteContext = {
  params: Promise<{ chainId: string; contractAddress: string; tokenId: string }>;
};

const getHandler = withAuthenticatedRoute<TokenOwnersRouteContext>(["owners:read"], async ({ context, request }) => {
  const params = await context.params;
  const parsedQueryResult = holderListQuerySchema.safeParse({
    cursor: request.nextUrl.searchParams.get("cursor") ?? undefined,
    limit: request.nextUrl.searchParams.get("limit") ?? undefined
  });

  if (!parsedQueryResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_holder_list_query",
      issues: buildValidationIssues(parsedQueryResult.error)
    });
  }

  const parsedQuery = parsedQueryResult.data;
  const chainId = Number(params.chainId);

  if (!Number.isInteger(chainId) || chainId <= 0) {
    return buildApiErrorResponse({
      error: "invalid_chain_id",
      message: "The chainId path parameter must be a positive integer.",
      status: 400
    });
  }

  const database = getWebMongoDatabase();
  const token = await findTokenByIdentity({
    database,
    chainId,
    contractAddress: params.contractAddress,
    tokenId: params.tokenId
  });

  if (!token) {
    return buildApiErrorResponse({
      error: "token_not_found",
      status: 404
    });
  }

  if (token.standard === "erc721") {
    const ownership = await findErc721OwnershipByToken({
      database,
      chainId,
      contractAddress: params.contractAddress,
      tokenId: params.tokenId
    });

    return buildApiSuccessResponse({
      standard: token.standard,
      items: ownership ? [serializeErc721OwnershipDocument(ownership)] : [],
      pageInfo: {
        limit: parsedQuery.limit,
        hasMore: false,
        nextCursor: null
      }
    });
  }

  if (token.standard !== "erc1155") {
    return buildApiErrorResponse({
      error: "unsupported_standard",
      message: "Holder listing is currently implemented for ERC-721 and ERC-1155 tokens only.",
      status: 400
    });
  }

  const listParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    chainId: number;
    contractAddress: string;
    tokenId: string;
    limit: number;
    cursor?: {
      updatedAt: Date;
      id: ObjectId;
    };
  } = {
    database,
    chainId,
    contractAddress: params.contractAddress,
    tokenId: params.tokenId,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.cursor) {
    const cursorResult = safeDecodeUpdatedAtCursor(parsedQuery.cursor);

    if (!cursorResult.ok) {
      return cursorResult.response;
    }

    listParams.cursor = cursorResult.value;
  }

  const balances = await listErc1155Balances(listParams);
  const hasMore = balances.length > parsedQuery.limit;
  const pageBalances = hasMore ? balances.slice(0, parsedQuery.limit) : balances;
  const lastPageBalance = pageBalances.at(-1);
  const nextCursor = hasMore && lastPageBalance ? encodeUpdatedAtCursor(lastPageBalance) : null;

  return buildApiSuccessResponse({
    standard: token.standard,
    items: pageBalances.map(serializeErc1155BalanceDocument),
    pageInfo: {
      limit: parsedQuery.limit,
      hasMore,
      nextCursor
    }
  });
});

export async function GET(request: NextRequest, context: TokenOwnersRouteContext): Promise<Response> {
  return getHandler(request, context);
}