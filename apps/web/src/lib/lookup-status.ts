export type PublicLookupStatus = "found" | "not_found" | "not_requested";

export function buildCollectionLookup(hasCollection: boolean) {
  return {
    collectionStatus: hasCollection ? ("found" as const) : ("not_found" as const),
    tokenStatus: "not_requested" as const
  };
}

export function buildTokenLookup(params: { hasCollection: boolean; hasToken: boolean }) {
  return {
    collectionStatus: params.hasCollection ? ("found" as const) : ("not_found" as const),
    tokenStatus: params.hasToken ? ("found" as const) : ("not_found" as const)
  };
}