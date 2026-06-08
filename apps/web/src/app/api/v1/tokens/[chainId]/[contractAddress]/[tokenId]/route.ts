import type { NextRequest } from "next/server";
import { findCollectionByIdentity, findTokenByIdentity } from "@nft-platform/db";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../../../../lib/api-response";
import { getWebMongoDatabase } from "../../../../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../../../../lib/api-auth";
import { serializeEnrichedCollection } from "../../../../../../../lib/collection-response";
import { buildTokenLookup } from "../../../../../../../lib/lookup-status";
import { serializeEnrichedToken } from "../../../../../../../lib/token-response";

export const dynamic = "force-dynamic";

type TokenRouteContext = {
  params: Promise<{ chainId: string; contractAddress: string; tokenId: string }>;
};

const getHandler = withAuthenticatedRoute<TokenRouteContext>(
  ["tokens:read"],
  async ({ context }) => {
    const params = await context.params;
    const chainId = Number(params.chainId);

    if (!Number.isInteger(chainId) || chainId <= 0) {
      return buildApiErrorResponse({
        error: "invalid_chain_id",
        message: "The chainId path parameter must be a positive integer.",
        status: 400
      });
    }

    const database = getWebMongoDatabase();
    const [collection, token] = await Promise.all([
      findCollectionByIdentity({
        database,
        chainId,
        contractAddress: params.contractAddress
      }),
      findTokenByIdentity({
        database,
        chainId,
        contractAddress: params.contractAddress,
        tokenId: params.tokenId
      })
    ]);

    const lookup = buildTokenLookup({
      hasCollection: Boolean(collection),
      hasToken: Boolean(token)
    });

    const serializedCollection = collection ? await serializeEnrichedCollection(database, collection) : null;

    if (!token) {
      return buildApiErrorResponse({
        error: "token_not_found",
        message: collection
          ? "The collection exists, but the requested token could not be confirmed from public data."
          : "Neither the requested collection nor the requested token could be confirmed from public data.",
        status: 404,
        details: {
          lookup,
          requestedIdentity: {
            chainId,
            contractAddress: params.contractAddress,
            tokenId: params.tokenId
          },
          collection: serializedCollection,
          item: null
        }
      });
    }

    return buildApiSuccessResponse({
      lookup,
      requestedIdentity: {
        chainId,
        contractAddress: params.contractAddress,
        tokenId: params.tokenId
      },
      collection: serializedCollection,
      item: await serializeEnrichedToken(database, token)
    });
  }
);

export async function GET(request: NextRequest, context: TokenRouteContext): Promise<Response> {
  return getHandler(request, context);
}