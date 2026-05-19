import type { NextRequest } from "next/server";
import { evmAddressSchema, supportedChainIds } from "@nft-platform/domain";
import { buildValidationErrorResponse, buildValidationIssues, safeDecodeUpdatedAtCursor } from "../../../../../../lib/api-validation";
import { withAuthenticatedRoute } from "../../../../../../lib/api-auth";
import { getWebMongoDatabase } from "../../../../../../lib/mongodb";
import { loadWalletInventory, ownerInventoryQuerySchema } from "../../../../../../lib/owner-inventory";

export const dynamic = "force-dynamic";

type MultiChainWalletOwnersRouteContext = {
  params: Promise<{ ownerAddress: string }>;
};

const getHandler = withAuthenticatedRoute<MultiChainWalletOwnersRouteContext>(["owners:read"], async ({ context, request }) => {
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

  const parsedOwnerAddress = evmAddressSchema.safeParse(params.ownerAddress);

  if (!parsedOwnerAddress.success) {
    return Response.json(
      {
        ok: false,
        error: "invalid_owner_address",
        message: "The ownerAddress path parameter must be a valid EVM address."
      },
      { status: 400 }
    );
  }

  const parsedChainIds = parseRequestedChainIds(request.nextUrl.searchParams);

  if (!parsedChainIds.ok) {
    return Response.json(
      {
        ok: false,
        error: parsedChainIds.error,
        message: parsedChainIds.message,
        supportedChainIds
      },
      { status: 400 }
    );
  }

  let cursor;

  if (parsedQueryResult.data.cursor) {
    const cursorResult = safeDecodeUpdatedAtCursor(parsedQueryResult.data.cursor);

    if (!cursorResult.ok) {
      return cursorResult.response;
    }

    cursor = cursorResult.value;
  }

  const inventory = await loadWalletInventory({
    database: getWebMongoDatabase(),
    chainIds: parsedChainIds.value,
    ownerAddress: parsedOwnerAddress.data,
    ...(parsedQueryResult.data.standard ? { standard: parsedQueryResult.data.standard } : {}),
    ...(parsedQueryResult.data.contractAddress ? { contractAddress: parsedQueryResult.data.contractAddress } : {}),
    ...(parsedQueryResult.data.metadataStatus ? { metadataStatus: parsedQueryResult.data.metadataStatus } : {}),
    ...(parsedQueryResult.data.mediaStatus ? { mediaStatus: parsedQueryResult.data.mediaStatus } : {}),
    ...(parsedQueryResult.data.traitType ? { traitType: parsedQueryResult.data.traitType } : {}),
    ...(parsedQueryResult.data.traitValue ? { traitValue: parsedQueryResult.data.traitValue } : {}),
    ...(parsedQueryResult.data.q ? { queryText: parsedQueryResult.data.q } : {}),
    limit: parsedQueryResult.data.limit,
    ...(cursor ? { cursor } : {})
  });

  return Response.json({
    ok: true,
    chainIds: parsedChainIds.value,
    ...inventory
  });
});

function parseRequestedChainIds(searchParams: URLSearchParams):
  | { ok: true; value: number[] }
  | { ok: false; error: string; message: string } {
  const repeatedChainIds = searchParams.getAll("chainId");
  const commaSeparatedChainIds = (searchParams.get("chainIds") ?? "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const rawChainIds = [...repeatedChainIds, ...commaSeparatedChainIds];

  if (rawChainIds.length === 0) {
    return {
      ok: true,
      value: supportedChainIds
    };
  }

  const chainIds = rawChainIds.map((value) => Number(value));

  if (chainIds.some((value) => !Number.isInteger(value) || value <= 0)) {
    return {
      ok: false,
      error: "invalid_chain_ids",
      message: "chainId and chainIds must contain only positive integer chain ids."
    };
  }

  const supportedChainIdSet = new Set<number>(supportedChainIds);
  const unsupportedChainIds = [...new Set(chainIds.filter((value) => !supportedChainIdSet.has(value)))];

  if (unsupportedChainIds.length > 0) {
    return {
      ok: false,
      error: "unsupported_chain_ids",
      message: `Unsupported chain ids requested: ${unsupportedChainIds.join(", ")}.`
    };
  }

  return {
    ok: true,
    value: [...new Set(chainIds)]
  };
}

export async function GET(request: NextRequest, context: MultiChainWalletOwnersRouteContext): Promise<Response> {
  return getHandler(request, context);
}