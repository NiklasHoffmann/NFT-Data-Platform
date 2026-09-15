import { findCollectionByIdentity } from "@nft-platform/db";
import { getWebMongoDatabase } from "./mongodb";
import { buildApiErrorResponse } from "./api-response";

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

  if (collection?.syncStatus !== "disabled") {
    return { ok: true };
  }

  return {
    ok: false,
    response: buildApiErrorResponse({
      error: "collection_disabled",
      message:
        "This collection is disabled and is not being indexed. Re-enable it through POST /api/v1/collections/{chainId}/{contractAddress}/status before requesting work for it.",
      status: 409
    })
  };
}
