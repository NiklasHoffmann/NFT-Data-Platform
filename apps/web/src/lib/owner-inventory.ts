import {
  findTokensByIdentities,
  getMongoCollections,
  listErc721OwnershipByOwner,
  listErc1155BalancesByOwner,
  serializeCollectionDocument,
  serializeErc721OwnershipDocument,
  serializeErc1155BalanceDocument
} from "@nft-platform/db";
import {
  evmAddressSchema,
  mediaStatusSchema,
  metadataStatusSchema,
  nftStandardSchema
} from "@nft-platform/domain";
import type { Db } from "mongodb";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { encodeUpdatedAtCursor } from "./cursor-pagination";
import { serializeEnrichedTokens } from "./token-response";

export type OwnerInventoryCursor = {
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

export const ownerInventoryQuerySchema = z.object({
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

type OwnerInventoryQuery = z.infer<typeof ownerInventoryQuerySchema>;

export type WalletCollectionSummary = {
  _id: string;
  chainId: number;
  contractAddress: string;
  standard: string;
  name: string | null;
  symbol: string | null;
  description: string | null;
  externalUrl: string | null;
  imageOriginalUrl: string | null;
  bannerImageOriginalUrl: string | null;
  featuredImageOriginalUrl: string | null;
  animationOriginalUrl: string | null;
  audioOriginalUrl: string | null;
  interactiveOriginalUrl: string | null;
  indexedTokenCount: number;
  syncStatus: string;
  updatedAt: Date;
};

export async function loadWalletInventory(params: {
  database: Db;
  chainIds: number[];
  ownerAddress: string;
  standard?: NonNullable<OwnerInventoryQuery["standard"]>;
  contractAddress?: string;
  metadataStatus?: NonNullable<OwnerInventoryQuery["metadataStatus"]>;
  mediaStatus?: NonNullable<OwnerInventoryQuery["mediaStatus"]>;
  traitType?: string;
  traitValue?: string;
  queryText?: string;
  limit: number;
  cursor?: OwnerInventoryCursor;
}) {
  const chainIds = [...new Set(params.chainIds)];
  const parsedTraitValue = parseTraitValue(params.traitValue);
  const mergedItems = (
    await Promise.all(
      chainIds.map(async (chainId) => {
        const listParams: {
          database: Db;
          chainId: number;
          ownerAddress: string;
          contractAddress?: string;
          metadataStatus?: NonNullable<OwnerInventoryQuery["metadataStatus"]>;
          mediaStatus?: NonNullable<OwnerInventoryQuery["mediaStatus"]>;
          traitType?: string;
          traitValue?: string | number | boolean;
          queryText?: string;
          limit: number;
          cursor?: OwnerInventoryCursor;
        } = {
          database: params.database,
          chainId,
          ownerAddress: params.ownerAddress,
          limit: params.limit + 1
        };

        if (params.contractAddress) {
          listParams.contractAddress = params.contractAddress;
        }

        if (params.metadataStatus) {
          listParams.metadataStatus = params.metadataStatus;
        }

        if (params.mediaStatus) {
          listParams.mediaStatus = params.mediaStatus;
        }

        if (params.traitType) {
          listParams.traitType = params.traitType;
        }

        if (parsedTraitValue !== undefined) {
          listParams.traitValue = parsedTraitValue;
        }

        if (params.queryText) {
          listParams.queryText = params.queryText;
        }

        if (params.cursor) {
          listParams.cursor = params.cursor;
        }

        const [erc721Ownership, erc1155Balances] = await Promise.all([
          params.standard !== "erc1155" ? listErc721OwnershipByOwner(listParams) : Promise.resolve([]),
          params.standard !== "erc721" ? listErc1155BalancesByOwner(listParams) : Promise.resolve([])
        ]);

        return [
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
        ];
      })
    )
  )
    .flat()
    .sort(compareOwnerInventoryItems);

  const hasMore = mergedItems.length > params.limit;
  const pageItems = hasMore ? mergedItems.slice(0, params.limit) : mergedItems;
  const lastPageItem = pageItems.at(-1);
  const nextCursor = hasMore && lastPageItem ? encodeUpdatedAtCursor(lastPageItem.document) : null;
  const [serializedTokens, collectionsByIdentity] = await Promise.all([
    pageItems.length > 0
      ? findTokensByIdentities({
          database: params.database,
          identities: pageItems.map((item) => ({
            chainId: item.document.chainId,
            contractAddress: item.document.contractAddress,
            tokenId: item.document.tokenId
          }))
        }).then((tokens) => serializeEnrichedTokens(params.database, tokens))
      : Promise.resolve([]),
    loadWalletCollectionSummaries(params.database, pageItems)
  ]);
  const tokensByIdentity = new Map(
    serializedTokens.map((token) => [`${token.chainId}:${token.contractAddress}:${token.tokenId}`, token])
  );

  return {
    standard: params.standard ?? "all",
    items: pageItems.map((item) => {
      const tokenIdentity = `${item.document.chainId}:${item.document.contractAddress}:${item.document.tokenId}`;
      const collectionIdentity = `${item.document.chainId}:${item.document.contractAddress}`;
      const token = tokensByIdentity.get(tokenIdentity) ?? null;
      const collection = collectionsByIdentity.get(collectionIdentity) ?? null;

      if (item.standard === "erc721") {
        return {
          standard: item.standard,
          ...serializeErc721OwnershipDocument(item.document),
          token,
          collection
        };
      }

      return {
        standard: item.standard,
        ...serializeErc1155BalanceDocument(item.document),
        token,
        collection
      };
    }),
    pageInfo: {
      limit: params.limit,
      hasMore,
      nextCursor
    }
  };
}

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

async function loadWalletCollectionSummaries(database: Db, items: OwnerInventoryItem[]) {
  const collectionIdentityMap = new Map<string, { chainId: number; contractAddress: string }>();

  for (const item of items) {
    const identity = `${item.document.chainId}:${item.document.contractAddress}`;

    if (!collectionIdentityMap.has(identity)) {
      collectionIdentityMap.set(identity, {
        chainId: item.document.chainId,
        contractAddress: item.document.contractAddress
      });
    }
  }

  const collectionIdentities = [...collectionIdentityMap.values()];

  if (collectionIdentities.length === 0) {
    return new Map<string, WalletCollectionSummary>();
  }

  const collectionDocuments = await getMongoCollections(database)
    .collections.find({
      $or: collectionIdentities
    })
    .toArray();
  const collectionSummaries = collectionDocuments.map(buildWalletCollectionSummary);

  return new Map(
    collectionSummaries.map((collection) => [
      `${collection.chainId}:${collection.contractAddress}`,
      collection
    ])
  );
}

function buildWalletCollectionSummary(collection: Parameters<typeof serializeCollectionDocument>[0]): WalletCollectionSummary {
  const serializedCollection = serializeCollectionDocument(collection);

  return {
    _id: serializedCollection._id,
    chainId: serializedCollection.chainId,
    contractAddress: serializedCollection.contractAddress,
    standard: serializedCollection.standard,
    name: serializedCollection.name,
    symbol: serializedCollection.symbol,
    description: serializedCollection.description,
    externalUrl: serializedCollection.externalUrl,
    imageOriginalUrl: serializedCollection.imageOriginalUrl,
    bannerImageOriginalUrl: serializedCollection.bannerImageOriginalUrl,
    featuredImageOriginalUrl: serializedCollection.featuredImageOriginalUrl,
    animationOriginalUrl: serializedCollection.animationOriginalUrl,
    audioOriginalUrl: serializedCollection.audioOriginalUrl,
    interactiveOriginalUrl: serializedCollection.interactiveOriginalUrl,
    indexedTokenCount: serializedCollection.indexedTokenCount,
    syncStatus: serializedCollection.syncStatus,
    updatedAt: serializedCollection.updatedAt
  };
}

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