import type { NextRequest } from "next/server";
import { ObjectId } from "mongodb";
import { listCollections } from "@nft-platform/db";
import { safeDecodeUpdatedAtCursor } from "../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { decodeUpdatedAtCursor, encodeUpdatedAtCursor } from "../../../../lib/cursor-pagination";
import { serializeEnrichedCollections } from "../../../../lib/collection-response";

export const dynamic = "force-dynamic";

const collectionListQuerySchema = Object.freeze({
  parse(searchParams: URLSearchParams) {
    const limitParam = Number(searchParams.get("limit") ?? "50");
    const limit = Number.isFinite(limitParam) ? Math.max(1, Math.min(limitParam, 200)) : 50;

    return {
      limit,
      cursor: searchParams.get("cursor") ?? undefined
    };
  }
});

const getHandler = withAuthenticatedRoute(["collections:read"], async ({ request }) => {
  const parsedQuery = collectionListQuerySchema.parse(request.nextUrl.searchParams);
  const database = getWebMongoDatabase();
  const collectionListParams: {
    database: ReturnType<typeof getWebMongoDatabase>;
    limit: number;
    cursor?: {
      updatedAt: Date;
      id: ObjectId;
    };
  } = {
    database,
    limit: parsedQuery.limit + 1
  };

  if (parsedQuery.cursor) {
    const cursorResult = safeDecodeUpdatedAtCursor(parsedQuery.cursor);

    if (!cursorResult.ok) {
      return cursorResult.response;
    }

    collectionListParams.cursor = cursorResult.value;
  }

  const collections = await listCollections({
    ...collectionListParams
  });
  const hasMore = collections.length > parsedQuery.limit;
  const pageCollections = hasMore ? collections.slice(0, parsedQuery.limit) : collections;
  const lastPageCollection = pageCollections.at(-1);
  const nextCursor = hasMore && lastPageCollection ? encodeUpdatedAtCursor(lastPageCollection) : null;

  return Response.json({
    ok: true,
    items: await serializeEnrichedCollections(database, pageCollections),
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
