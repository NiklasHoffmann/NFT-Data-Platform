import type { NextRequest } from "next/server";
import { evmAddressSchema, mediaStatusSchema, metadataStatusSchema, nftStandardSchema } from "@nft-platform/domain";
import {
  findTokensByIdentities,
  listErc721OwnershipByOwner,
  listErc1155BalancesByOwner,
  serializeErc721OwnershipDocument,
  serializeErc1155BalanceDocument
} from "@nft-platform/db";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { withAuthenticatedRoute } from "../../../../../../../lib/api-auth";
import { decodeUpdatedAtCursor, encodeUpdatedAtCursor } from "../../../../../../../lib/cursor-pagination";
import { getWebMongoDatabase } from "../../../../../../../lib/mongodb";
import { serializeEnrichedTokens } from "../../../../../../../lib/token-response";

export const dynamic = "force-dynamic";

type OwnerInventoryCursor = {
  updatedAt: Date;
  id: ObjectId;
};

type Erc721OwnerInventoryDocument = Awaited<ReturnType<typeof listErc721OwnershipByOwner>>[number];
type Erc1155OwnerInventoryDocument = Awaited<ReturnType<typeof listErc1155BalancesByOwner>>[number];

type OwnerInventoryItem =
  | {
      standard: "erc721";
      document: Erc721OwnerInventoryDocument;
    }
  | {
      standard: "erc1155";
      document: Erc1155OwnerInventoryDocument;
    };

function compareOwnerInventoryItems(left: OwnerInventoryItem, right: OwnerInventoryItem): number {
  const updatedAtDifference = right.document.updatedAt.getTime() - left.document.updatedAt.getTime();

  if (updatedAtDifference !== 0) {
    return updatedAtDifference;
  }

  const leftId = left.document._id.toHexString();
  const rightId = right.document._id.toHexString();

  if (leftId === rightId) {
    return 0;
  }

  return leftId < rightId ? 1 : -1;
}

const ownerInventoryQuerySchema = z.object({
  q: z.string().trim().min(1).optional(),
  standard: nftStandardSchema.optional(),
  contractAddress: evmAddressSchema.optional(),
  metadataStatus: metadataStatusSchema.optional(),
  mediaStatus: mediaStatusSchema.optional(),
  traitType: z.string().trim().min(1).optional(),
  traitValue: z.string().trim().min(1).optional(),
  cursor: z.string().trim().min(1).optional(),
  limit: z.coerce.number().int().positive().max(200).default(50)
});

type WalletOwnersRouteContext = {
  params: Promise<{ chainId: string; ownerAddress: string }>;
};

const getHandler = withAuthenticatedRoute<WalletOwnersRouteContext>(["owners:read"], async ({ context, request }) => {
  const params = await context.params;
  const parsedQuery = ownerInventoryQuerySchema.parse({
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
  const chainId = Number(params.chainId);

  if (!Number.isInteger(chainId) || chainId <= 0) {
    return Response.json(
      {
        ok: false,
        error: "invalid_chain_id",
        message: "The chainId path parameter must be a positive integer."
      },
      { status: 400 }
    );
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

  const database = getWebMongoDatabase();
  const listParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    chainId: number;
    ownerAddress: string;
    contractAddress?: string;
    metadataStatus?: z.infer<typeof metadataStatusSchema>;
    mediaStatus?: z.infer<typeof mediaStatusSchema>;
    traitType?: string;
    traitValue?: string | number | boolean;
    queryText?: string;
    limit: number;
    cursor?: OwnerInventoryCursor;
  } = {
    database,
    chainId,
    ownerAddress: parsedOwnerAddress.data,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.contractAddress) {
    listParams.contractAddress = parsedQuery.contractAddress;
  }

  if (parsedQuery.metadataStatus) {
    listParams.metadataStatus = parsedQuery.metadataStatus;
  }

  if (parsedQuery.mediaStatus) {
    listParams.mediaStatus = parsedQuery.mediaStatus;
  }

  if (parsedQuery.traitType) {
    listParams.traitType = parsedQuery.traitType;
  }

  const parsedTraitValue = parseTraitValue(parsedQuery.traitValue);

  if (parsedTraitValue !== undefined) {
    listParams.traitValue = parsedTraitValue;
  }

  if (parsedQuery.q) {
    listParams.queryText = parsedQuery.q;
  }

  if (parsedQuery.cursor) {
    listParams.cursor = decodeUpdatedAtCursor(parsedQuery.cursor);
  }

  const [erc721Ownership, erc1155Balances] = await Promise.all([
    parsedQuery.standard !== "erc1155"
      ? listErc721OwnershipByOwner(listParams)
      : Promise.resolve([]),
    parsedQuery.standard !== "erc721"
      ? listErc1155BalancesByOwner(listParams)
      : Promise.resolve([])
  ]);
  const mergedItems = [
    ...erc721Ownership.map(
      (document): OwnerInventoryItem => ({
        standard: "erc721",
        document
      })
    ),
    ...erc1155Balances.map(
      (document): OwnerInventoryItem => ({
        standard: "erc1155",
        document
      })
    )
  ].sort(compareOwnerInventoryItems);
  const hasMore = mergedItems.length > parsedQuery.limit;
  const pageItems = hasMore ? mergedItems.slice(0, parsedQuery.limit) : mergedItems;
  const lastPageItem = pageItems.at(-1);
  const nextCursor = hasMore && lastPageItem ? encodeUpdatedAtCursor(lastPageItem.document) : null;
  const tokens = await findTokensByIdentities({
    database,
    identities: pageItems.map((item) => ({
      chainId: item.document.chainId,
      contractAddress: item.document.contractAddress,
      tokenId: item.document.tokenId
    }))
  });
  const serializedTokens = await serializeEnrichedTokens(database, tokens);
  const tokensByIdentity = new Map(
    serializedTokens.map((token) => [`${token.chainId}:${token.contractAddress}:${token.tokenId}`, token])
  );

  return Response.json({
    ok: true,
    standard: parsedQuery.standard ?? "all",
    items: pageItems.map((item) => {
      const token = tokensByIdentity.get(
        `${item.document.chainId}:${item.document.contractAddress}:${item.document.tokenId}`
      ) ?? null;

      if (item.standard === "erc721") {
        return {
          standard: item.standard,
          ...serializeErc721OwnershipDocument(item.document),
          token
        };
      }

      return {
        standard: item.standard,
        ...serializeErc1155BalanceDocument(item.document),
        token
      };
    }),
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

export async function GET(request: NextRequest, context: WalletOwnersRouteContext): Promise<Response> {
  return getHandler(request, context);
}