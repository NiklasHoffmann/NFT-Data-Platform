import type { NextRequest } from "next/server";
import { z } from "zod";
import { findCollectionByIdentity, setCollectionSyncStatus } from "@nft-platform/db";
import { withAuthenticatedRoute } from "../../../../../../../lib/api-auth";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../../../../lib/mongodb";

export const dynamic = "force-dynamic";

type CollectionStatusRouteContext = { params: Promise<{ chainId: string; contractAddress: string }> };

/**
 * Only the two lifecycle ends are settable from outside. `pending`, `syncing` and `error` are
 * states the worker moves through on its own, and letting a client write them would desynchronise
 * the indexing loop from what it believes about its own progress.
 */
const collectionStatusRequestSchema = z.object({
  syncStatus: z.enum(["active", "disabled"])
});

const postHandler = withAuthenticatedRoute<CollectionStatusRouteContext>(
  ["collections:write"],
  async ({ auth, context }) => {
    const params = await context.params;
    const chainId = Number(params.chainId);

    if (!Number.isInteger(chainId) || chainId <= 0) {
      return buildApiErrorResponse({
        error: "invalid_chain_id",
        message: "The chainId path parameter must be a positive integer.",
        status: 400
      });
    }

    const parsedBody = safeParseJsonRequestBody(auth.bodyText);

    if (!parsedBody.ok) {
      return parsedBody.response;
    }

    const validatedBody = collectionStatusRequestSchema.safeParse(parsedBody.data);

    if (!validatedBody.success) {
      return buildValidationErrorResponse({
        error: "invalid_collection_status_request",
        issues: buildValidationIssues(validatedBody.error)
      });
    }

    const database = getWebMongoDatabase();
    const existingCollection = await findCollectionByIdentity({
      database,
      chainId,
      contractAddress: params.contractAddress
    });

    // Registering a collection is what puts it into the index, so this endpoint deliberately does
    // not create one. Switching the status of something that was never registered would otherwise
    // read as success while nothing is being indexed.
    if (!existingCollection) {
      return buildApiErrorResponse({
        error: "collection_not_found",
        message: "Register the collection through POST /api/v1/refresh/collection before changing its status.",
        status: 404
      });
    }

    const updatedCollection = await setCollectionSyncStatus({
      database,
      chainId,
      contractAddress: params.contractAddress,
      syncStatus: validatedBody.data.syncStatus,
      updatedAt: new Date()
    });

    if (!updatedCollection) {
      return buildApiErrorResponse({
        error: "collection_not_found",
        status: 404
      });
    }

    return buildApiSuccessResponse({
      chainId: updatedCollection.chainId,
      contractAddress: updatedCollection.contractAddress,
      previousSyncStatus: existingCollection.syncStatus,
      syncStatus: updatedCollection.syncStatus,
      // Reactivation resumes from the stored checkpoint rather than starting over, so the size of
      // the gap the worker has to catch up on is part of the answer.
      deployBlock: updatedCollection.deployBlock,
      lastIndexedBlock: updatedCollection.lastIndexedBlock,
      lastObservedBlock: updatedCollection.lastObservedBlock
    });
  }
);

export async function POST(request: NextRequest, context: CollectionStatusRouteContext): Promise<Response> {
  return postHandler(request, context);
}
