import type { NextRequest } from "next/server";
import { evmAddressSchema } from "@nft-platform/domain";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeDecodeUpdatedAtCursor } from "../../../../../../../lib/api-validation";
import { withAuthenticatedRoute } from "../../../../../../../lib/api-auth";
import { getWebMongoDatabase } from "../../../../../../../lib/mongodb";
import { loadWalletInventory, ownerInventoryQuerySchema } from "../../../../../../../lib/owner-inventory";

export const dynamic = "force-dynamic";

type WalletOwnersRouteContext = {
  params: Promise<{ chainId: string; ownerAddress: string }>;
};

const getHandler = withAuthenticatedRoute<WalletOwnersRouteContext>(["owners:read"], async ({ context, request }) => {
  const params = await context.params;
  const parsedQueryResult = ownerInventoryQuerySchema.safeParse({
    q: request.nextUrl.searchParams.get("q") ?? undefined,
    standard: request.nextUrl.searchParams.get("standard") ?? undefined,
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
      error: "invalid_owner_inventory_query",
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

  const parsedOwnerAddress = evmAddressSchema.safeParse(params.ownerAddress);

  if (!parsedOwnerAddress.success) {
    return buildApiErrorResponse({
      error: "invalid_owner_address",
      message: "The ownerAddress path parameter must be a valid EVM address.",
      status: 400
    });
  }

  const database = getWebMongoDatabase();
  let cursor;

  if (parsedQuery.cursor) {
    const cursorResult = safeDecodeUpdatedAtCursor(parsedQuery.cursor);

    if (!cursorResult.ok) {
      return cursorResult.response;
    }

    cursor = cursorResult.value;
  }

  const inventory = await loadWalletInventory({
    database,
    chainIds: [chainId],
    ownerAddress: parsedOwnerAddress.data,
    ...(parsedQuery.standard ? { standard: parsedQuery.standard } : {}),
    ...(parsedQuery.contractAddress ? { contractAddress: parsedQuery.contractAddress } : {}),
    ...(parsedQuery.metadataStatus ? { metadataStatus: parsedQuery.metadataStatus } : {}),
    ...(parsedQuery.mediaStatus ? { mediaStatus: parsedQuery.mediaStatus } : {}),
    ...(parsedQuery.traitType ? { traitType: parsedQuery.traitType } : {}),
    ...(parsedQuery.traitValue ? { traitValue: parsedQuery.traitValue } : {}),
    ...(parsedQuery.q ? { queryText: parsedQuery.q } : {}),
    limit: parsedQuery.limit,
    ...(cursor ? { cursor } : {})
  });

  return buildApiSuccessResponse(inventory);
});

export async function GET(request: NextRequest, context: WalletOwnersRouteContext): Promise<Response> {
  return getHandler(request, context);
}