import type { NextRequest } from "next/server";
import { mediaStatusSchema, metadataStatusSchema } from "@nft-platform/domain";
import { listCollections, listTokens } from "@nft-platform/db";
import { ObjectId } from "mongodb";
import { z } from "zod";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { decodeUpdatedAtCursor, encodeUpdatedAtCursor } from "../../../../lib/cursor-pagination";
import { serializeEnrichedCollections } from "../../../../lib/collection-response";
import { serializeEnrichedTokens } from "../../../../lib/token-response";

export const dynamic = "force-dynamic";

const searchQuerySchema = z
  .object({
    q: z.string().trim().min(1),
    entity: z.enum(["tokens", "collections", "all"]).default("tokens"),
    chainId: z.coerce.number().int().positive().optional(),
    contractAddress: z.string().trim().regex(/^0x[a-fA-F0-9]{40}$/).optional(),
    metadataStatus: metadataStatusSchema.optional(),
    mediaStatus: mediaStatusSchema.optional(),
    cursor: z.string().trim().min(1).optional(),
    limit: z.coerce.number().int().positive().max(200).default(50)
  })
  .superRefine((value, context) => {
    if (value.entity !== "tokens" && value.metadataStatus) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["metadataStatus"],
        message: "metadataStatus is only supported when entity=tokens."
      });
    }

    if (value.entity !== "tokens" && value.mediaStatus) {
      context.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["mediaStatus"],
        message: "mediaStatus is only supported when entity=tokens."
      });
    }
  });

type SearchCursor = {
  updatedAt: Date;
  id: ObjectId;
};

type CollectionSearchDocument = Awaited<ReturnType<typeof listCollections>>[number];
type TokenSearchDocument = Awaited<ReturnType<typeof listTokens>>[number];

type MixedSearchItem =
  | {
      entity: "collection";
      document: CollectionSearchDocument;
    }
  | {
      entity: "token";
      document: TokenSearchDocument;
    };

function compareSearchItems(left: MixedSearchItem, right: MixedSearchItem): number {
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

const getHandler = withAuthenticatedRoute(["search:read"], async ({ request }) => {
  const parsedQueryResult = searchQuerySchema.safeParse({
    q: request.nextUrl.searchParams.get("q") ?? undefined,
    entity: request.nextUrl.searchParams.get("entity") ?? undefined,
    chainId: request.nextUrl.searchParams.get("chainId") ?? undefined,
    contractAddress: request.nextUrl.searchParams.get("contractAddress") ?? undefined,
    metadataStatus: request.nextUrl.searchParams.get("metadataStatus") ?? undefined,
    mediaStatus: request.nextUrl.searchParams.get("mediaStatus") ?? undefined,
    cursor: request.nextUrl.searchParams.get("cursor") ?? undefined,
    limit: request.nextUrl.searchParams.get("limit") ?? undefined
  });

  if (!parsedQueryResult.success) {
    return Response.json(
      {
        ok: false,
        error: "invalid_search_query",
        issues: parsedQueryResult.error.issues.map((issue) => ({
          path: issue.path.join("."),
          message: issue.message
        }))
      },
      { status: 400 }
    );
  }

  const parsedQuery = parsedQueryResult.data;
  const database = getWebMongoDatabase();
  const cursor = parsedQuery.cursor ? decodeUpdatedAtCursor(parsedQuery.cursor) : undefined;

  const collectionSearchParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    limit: number;
    queryText: string;
    chainId?: number;
    contractAddress?: string;
    cursor?: SearchCursor;
  } = {
    database,
    queryText: parsedQuery.q,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.chainId !== undefined) {
    collectionSearchParams.chainId = parsedQuery.chainId;
  }

  if (parsedQuery.contractAddress) {
    collectionSearchParams.contractAddress = parsedQuery.contractAddress;
  }

  if (cursor) {
    collectionSearchParams.cursor = cursor;
  }

  const tokenSearchParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    limit: number;
    queryText: string;
    chainId?: number;
    contractAddress?: string;
    metadataStatus?: z.infer<typeof metadataStatusSchema>;
    mediaStatus?: z.infer<typeof mediaStatusSchema>;
    cursor?: SearchCursor;
  } = {
    database,
    queryText: parsedQuery.q,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.chainId !== undefined) {
    tokenSearchParams.chainId = parsedQuery.chainId;
  }

  if (parsedQuery.contractAddress) {
    tokenSearchParams.contractAddress = parsedQuery.contractAddress;
  }

  if (parsedQuery.metadataStatus) {
    tokenSearchParams.metadataStatus = parsedQuery.metadataStatus;
  }

  if (parsedQuery.mediaStatus) {
    tokenSearchParams.mediaStatus = parsedQuery.mediaStatus;
  }

  if (cursor) {
    tokenSearchParams.cursor = cursor;
  }

  if (parsedQuery.entity === "collections") {
    const collections = await listCollections(collectionSearchParams);
    const hasMore = collections.length > parsedQuery.limit;
    const pageCollections = hasMore ? collections.slice(0, parsedQuery.limit) : collections;
    const lastPageCollection = pageCollections.at(-1);
    const nextCursor = hasMore && lastPageCollection ? encodeUpdatedAtCursor(lastPageCollection) : null;

    return Response.json({
      ok: true,
      query: parsedQuery.q,
      entity: parsedQuery.entity,
      items: await serializeEnrichedCollections(database, pageCollections),
      pageInfo: {
        limit: parsedQuery.limit,
        hasMore,
        nextCursor
      }
    });
  }

  if (parsedQuery.entity === "all") {
    const [collections, tokens] = await Promise.all([
      listCollections(collectionSearchParams),
      listTokens(tokenSearchParams)
    ]);
    const mergedItems = [
      ...collections.map(
        (document): MixedSearchItem => ({
          entity: "collection",
          document
        })
      ),
      ...tokens.map(
        (document): MixedSearchItem => ({
          entity: "token",
          document
        })
      )
    ].sort(compareSearchItems);
    const hasMore = mergedItems.length > parsedQuery.limit;
    const pageItems = hasMore ? mergedItems.slice(0, parsedQuery.limit) : mergedItems;
    const lastPageItem = pageItems.at(-1);
    const nextCursor = hasMore && lastPageItem ? encodeUpdatedAtCursor(lastPageItem.document) : null;
    const pageCollections = pageItems.flatMap((item) => (item.entity === "collection" ? [item.document] : []));
    const pageTokens = pageItems.flatMap((item) => (item.entity === "token" ? [item.document] : []));
    const [serializedCollections, serializedTokens] = await Promise.all([
      serializeEnrichedCollections(database, pageCollections),
      serializeEnrichedTokens(database, pageTokens)
    ]);
    const serializedCollectionsById = new Map(
      serializedCollections.map((collection) => [collection._id, collection])
    );
    const serializedTokensById = new Map(serializedTokens.map((token) => [token._id, token]));
    const serializedItems: Array<Record<string, unknown>> = [];

    for (const item of pageItems) {
      const documentId = item.document._id.toHexString();

      if (item.entity === "collection") {
        const serializedCollection = serializedCollectionsById.get(documentId);

        if (serializedCollection) {
          serializedItems.push({ entity: item.entity, ...serializedCollection });
        }

        continue;
      }

      const serializedToken = serializedTokensById.get(documentId);

      if (serializedToken) {
        serializedItems.push({ entity: item.entity, ...serializedToken });
      }
    }

    return Response.json({
      ok: true,
      query: parsedQuery.q,
      entity: parsedQuery.entity,
      items: serializedItems,
      pageInfo: {
        limit: parsedQuery.limit,
        hasMore,
        nextCursor
      }
    });
  }

  const tokens = await listTokens(tokenSearchParams);
  const hasMore = tokens.length > parsedQuery.limit;
  const pageTokens = hasMore ? tokens.slice(0, parsedQuery.limit) : tokens;
  const lastPageToken = pageTokens.at(-1);
  const nextCursor = hasMore && lastPageToken ? encodeUpdatedAtCursor(lastPageToken) : null;

  return Response.json({
    ok: true,
    query: parsedQuery.q,
    entity: parsedQuery.entity,
    items: await serializeEnrichedTokens(database, pageTokens),
    pageInfo: {
      limit: parsedQuery.limit,
      hasMore,
      nextCursor
    }
  });
});

export async function GET(request: NextRequest): Promise<Response> {
  return getHandler(request, undefined);
}