import type { NextRequest } from "next/server";
import {
  findCollectionByIdentity,
  serializeCollectionDocument
} from "@nft-platform/db";
import { getWebMongoDatabase } from "../../../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../../../lib/api-auth";
import { serializeEnrichedCollection } from "../../../../../../lib/collection-response";
import { buildCollectionLookup } from "../../../../../../lib/lookup-status";

export const dynamic = "force-dynamic";

type CollectionRouteContext = { params: Promise<{ chainId: string; contractAddress: string }> };

const getHandler = withAuthenticatedRoute<CollectionRouteContext>(
  ["collections:read"],
  async ({ context }) => {
    const params = await context.params;
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

    const database = getWebMongoDatabase();
    const collection = await findCollectionByIdentity({
      database,
      chainId,
      contractAddress: params.contractAddress
    });

    if (!collection) {
      return Response.json(
        {
          ok: false,
          error: "collection_not_found"
          ,lookup: buildCollectionLookup(false),
          requestedIdentity: {
            chainId,
            contractAddress: params.contractAddress
          },
          item: null
        },
        { status: 404 }
      );
    }

    return Response.json({
      ok: true,
      lookup: buildCollectionLookup(true),
      requestedIdentity: {
        chainId,
        contractAddress: params.contractAddress
      },
      item: await serializeEnrichedCollection(database, collection)
    });
  }
);

export async function GET(request: NextRequest, context: CollectionRouteContext): Promise<Response> {
  return getHandler(request, context);
}