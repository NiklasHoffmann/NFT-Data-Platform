import { findCollectionByIdentity, type CollectionDocument } from "@nft-platform/db";
import { getWebMongoDatabase } from "./mongodb";
import { buildApiErrorResponse } from "./api-response";

/**
 * Said the same way by the `409` of the refresh endpoints and by the per-item result of the
 * discover endpoints, so whoever reads it learns the same thing wherever the switch stopped work.
 */
export const collectionDisabledMessage =
  "This collection is disabled and is not being indexed. Re-enable it through POST /api/v1/collections/{chainId}/{contractAddress}/status before requesting work for it.";

/**
 * The discover endpoints answer per requested item instead of rejecting the whole call, so they
 * need the check without the response. They already hold the collection documents they looked up.
 */
export function isDisabledCollection(collection: Pick<CollectionDocument, "syncStatus"> | null): boolean {
  return collection?.syncStatus === "disabled";
}

/**
 * The worker skips every job that belongs to a disabled collection, so accepting one here would
 * hand the caller a job id for work that is never going to happen. Rejecting at the edge is what
 * makes the shutdown visible instead of silent.
 *
 * A collection that does not exist yet passes: `POST /api/v1/refresh/collection` is also the
 * registration call, and registration has to stay possible.
 */
export async function guardCollectionNotDisabled(params: {
  chainId: number;
  contractAddress: string;
}): Promise<{ ok: true } | { ok: false; response: Response }> {
  const collection = await findCollectionByIdentity({
    database: getWebMongoDatabase(),
    chainId: params.chainId,
    contractAddress: params.contractAddress
  });

  if (!isDisabledCollection(collection)) {
    return { ok: true };
  }

  return {
    ok: false,
    response: buildApiErrorResponse({
      error: "collection_disabled",
      message: collectionDisabledMessage,
      status: 409
    })
  };
}
