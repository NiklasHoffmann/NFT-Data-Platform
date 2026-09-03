# NFT Data Platform Marketplace Migration

## Repository Status At A Glance

Marketplace-side file status confirmed against the marketplace repository on 2026-08-27.

Platform-side API contracts re-verified against a running NFT Data Platform instance on
2026-09-03. Everything below that describes `/api/v1/*` was checked against real responses, not
against an earlier draft of the platform docs. See `nft-api.md` and `wallet-api.md` in the platform
repository for the authoritative endpoint reference.

Status meaning:

- `[x]` already matches the target responsibility and should be kept
- `[ ]` must still be migrated for the NFT Data Platform cutover

### Already aligned with the target architecture

- `[x]` `src/services/nft-sync/graph-subscription.ts` - already treats TheGraph as listing source and writes listing-only data into `marketplace_items`
- `[x]` `src/config/apolloClient.ts` - keep Apollo scoped to subgraph usage only; do not extend it for NFT Data Platform reads
- `[x]` `src/app/api/marketplace/listing/[contractAddress]/[tokenId]/route.ts` - already serves listing-only data from TheGraph
- `[x]` `src/app/api/nft/stats/route.ts` - local social stats remain app-owned and should stay local
- `[x]` `src/app/api/user/interactions/route.ts` - likes, ratings, watchlist, and personal interaction data remain app-owned and should stay local

### Still open migration work

- `[ ]` `src/app/api/wallet/nfts/route.ts` - still uses Alchemy, Moralis, blockchain fallback, and DB persistence as the primary wallet read flow
- `[ ]` `src/app/api/user/nfts/sync/route.ts` - still uses Alchemy, Moralis, direct blockchain metadata fetches, and local DB enrichment as the primary sync flow
- `[ ]` `src/app/api/nft/detail/route.ts` - still reads `nft_metadata` first and performs synchronous blockchain and IPFS refresh behavior
- `[ ]` `src/app/api/collections/route.ts` - still aggregates collection view data from `marketplace_items` plus `nft_metadata`
- `[ ]` `src/contexts/wallet-nfts/WalletNFTsService.ts` - public `WalletNFT` shape can stay, but the service still assumes legacy external sources
- `[ ]` `src/hooks/marketplace/useMarketplaceItemDetail.ts` - still assumes legacy detail semantics and needs indexing-safe fallback handling
- `[ ]` `src/contexts/collections/CollectionsService.ts` - frontend contract can stay, but the backend source contract under `/api/collections` must change

## Purpose

This document describes what still needs to be implemented in this repository to use the marketplace UI with the **NFT Data Platform API** for NFT data, while **listing data continues to come from TheGraph**.

That means the target architecture is **hybrid**, not API-only:

- **TheGraph** remains the source for marketplace listings and listing status
- **NFT Data Platform** becomes the source for token details, collections, wallet inventory, and search
- **Local MongoDB** remains the source for NFT social stats and app-specific interaction data
- **Local MongoDB read models** should no longer be the primary read source for wallet, token, collection, and search views

Important: the NFT Data Platform is **not** just a thin live on-chain RPC layer. Based on the shared API contracts, it serves an **indexed read model** for token, collection, ownership, metadata, and media state. It does **not** appear to be the source for marketplace-local stats such as likes, views, watchlist counts, or ratings.

## Final Target Architecture

### Data ownership by domain

| Domain | Target source | Notes |
| --- | --- | --- |
| Marketplace listings | TheGraph | Active listings, price, seller, listing status, listing type |
| NFT token detail | NFT Data Platform | `/api/v1/tokens/:chainId/:contractAddress/:tokenId` |
| NFT token lists | NFT Data Platform | `/api/v1/tokens` |
| Collections | NFT Data Platform | `/api/v1/collections/:chainId/:contractAddress` for one, `/api/v1/collections` to browse |
| Search | NFT Data Platform | `/api/v1/search` |
| Wallet inventory | NFT Data Platform | `/api/v1/owners/wallets/:ownerAddress` |
| Wallet discovery fallback | NFT Data Platform | `/api/v1/owners/wallets/discover` |
| Token backfill / refresh | NFT Data Platform | `/api/v1/refresh/token`, `/api/v1/refresh/collection` |
| NFT social stats | Local MongoDB (`nft_stats`) | Views, likes, watchlist count, rating aggregates |
| User likes, ratings, watchlist | Local marketplace app | Existing app-owned feature set can stay local |
| Admin insights | Local MongoDB | Marketplace-specific editorial enrichment |

### Important consequence

This repository **cannot be described as using only the NFT Data Platform API** as long as listing data is still sourced from TheGraph.

The correct goal is:

> **Use TheGraph for listings and NFT Data Platform for everything NFT-related around those listings.**

More precisely:

> **Use TheGraph for listings, NFT Data Platform for indexed token/collection/ownership data, and local MongoDB for stats plus marketplace-specific enrichment.**

## Current State In This Repository

The current codebase still mixes four different concerns in the same read flows:

- TheGraph for listings
- local MongoDB read models for enriched NFT data
- Alchemy and Moralis for wallet discovery
- direct blockchain and IPFS refresh logic for missing metadata

The most important current coupling points are:

- `src/app/api/wallet/nfts/route.ts`
- `src/app/api/user/nfts/sync/route.ts`
- `src/app/api/nft/detail/route.ts`
- `src/app/api/collections/route.ts`
- `src/contexts/wallet-nfts/WalletNFTsService.ts`
- `src/contexts/collections/CollectionsService.ts`
- `src/hooks/marketplace/useMarketplaceItemDetail.ts`
- `src/services/nft-sync/graph-subscription.ts`
- `src/config/apolloClient.ts`

## Repository-Level Decisions Fixed By This Document

The points below are no longer open questions for this repository. They should be implemented as written unless upstream NFT Data Platform contracts force a change.

1. Browser components continue to call only local marketplace routes. No browser code may call the NFT Data Platform directly.
2. TheGraph remains the only source for listing truth. It must not regain responsibility for token metadata, wallet holdings, or collection enrichment.
3. Local MongoDB remains the only source for `nft_stats`, `user_likes`, `user_watchlist`, `user_ratings`, `personal_notes`, and admin insights.
4. All raw NFT Data Platform payloads must be normalized inside a server-only client layer before route code maps them into existing frontend contracts.
5. The current collections page is treated as a marketplace-listed-collections view, not a global collection directory. Therefore `/api/collections` should derive which contracts are relevant from active listings, then hydrate collection and token details from the NFT Data Platform.
6. `404` from NFT Data Platform token detail means `not indexed yet` by default, not automatically `token does not exist`.

## What Must Be Implemented

## Required New Server Modules

The migration should introduce the following modules explicitly instead of spreading platform logic across route files.

| Suggested file | Responsibility |
| --- | --- |
| `src/lib/nft-data-platform/client.ts` | server-only HTTP client, HMAC signing, retries, timeout handling, JSON parsing |
| `src/lib/nft-data-platform/types.ts` | normalized internal DTOs returned by the client |
| `src/lib/nft-data-platform/errors.ts` | platform error normalization (`404`, auth, validation, throttling, upstream outage) |
| `src/lib/nft-data-platform/mappers/wallet.ts` | normalized wallet holding -> current `WalletNFT` contract |
| `src/lib/nft-data-platform/mappers/token-detail.ts` | normalized token detail -> current detail API payload / `EnrichedNFTDocument`-oriented shape |
| `src/lib/nft-data-platform/mappers/collection.ts` | normalized collection and token summary -> current collections UI contract |
| `src/services/marketplace/marketplace-view-composer.ts` | merge layer for listing data + NFT Data Platform token data + local stats/insights |

## Required Normalized Internal Contracts

Implementation inside this repo should not let route handlers depend on raw upstream payloads. The client layer should convert upstream responses into a normalized internal contract first.

```ts
export interface PlatformCollectionSummary {
  chainId: number;
  contractAddress: string;
  name: string | null;
  symbol: string | null;
  imageUrl: string | null;
}

export interface PlatformTokenSummary {
  chainId: number;
  contractAddress: string;
  tokenId: string;
  standard: 'ERC721' | 'ERC1155' | null;
  name: string | null;
  description: string | null;
  imageUrl: string | null;
  animationUrl: string | null;
  externalUrl: string | null;
  attributes: Array<{ trait_type: string; value: unknown; display_type?: string }>;
  /**
   * Resolved metadata URI. Comes from `metadataUriResolved` on the platform token, with
   * `metadataUriRaw` as the unresolved original. There is no `tokenUri` field on the platform.
   */
  metadataUri: string | null;
  /**
   * ERC-1155 supply for this token id. Not the caller's balance.
   */
  supplyQuantity: string | null;
  collection: PlatformCollectionSummary | null;
  metadataState: PlatformMetadataState;
  mediaState: PlatformMediaState;
}

/**
 * Ownership is NOT part of a token read. The platform keeps ownership in separate read models and
 * exposes it through its own endpoints, so it has to be requested and merged deliberately.
 */
export interface PlatformTokenOwnership {
  chainId: number;
  contractAddress: string;
  tokenId: string;
  ownerAddress: string;
  /** Present for ERC-1155 holdings only. */
  balance: string | null;
}

/** Platform values, kept verbatim so no information is lost in normalization. */
export type PlatformMetadataState = 'pending' | 'ok' | 'failed' | 'stale';
export type PlatformMediaState = 'pending' | 'processing' | 'ready' | 'partial' | 'failed';

export interface PlatformWalletHolding {
  chainId: number;
  ownerAddress: string;
  contractAddress: string;
  tokenId: string;
  balance: string | null;
  token: PlatformTokenSummary | null;
  collection: PlatformCollectionSummary | null;
}

/**
 * The platform reports data age itself, so this does not need to be inferred. It maps directly
 * from the `freshness` block on a token detail response.
 */
export interface PlatformTokenFreshness {
  lastMetadataFetchAt: string | null;
  ageSeconds: number | null;
  isStale: boolean;
  /** True when reading the token caused the platform to queue its own refresh. */
  revalidationQueued: boolean;
}

export interface PlatformTokenDetail extends PlatformTokenSummary {
  freshness: PlatformTokenFreshness;
}

/** Cursor pagination envelope. The cursor is opaque and must not be parsed. */
export interface PlatformPage<T> {
  items: T[];
  hasMore: boolean;
  nextCursor: string | null;
}
```

Important: the exact HMAC header names, timestamp format, canonical string rules, and raw upstream field names are not repeated in this document because they belong only in `src/lib/nft-data-platform/client.ts`. The route layer should never know them.

## 1. Introduce a server-side NFT Data Platform client

The app currently has no dedicated client abstraction for the HMAC-authenticated NFT Data Platform API.

### Required work

- add a server-only API client module for signed requests
- centralize HMAC signing, error handling, timeout handling, and JSON parsing
- expose typed methods for the core read endpoints

### Suggested methods

- `getTokenList()`
- `getTokenDetail()`
- `getTokenOwners()`
- `listCollections()`
- `getCollectionDetail()`
- `searchEntities()`
- `getWalletInventory()`
- `discoverWalletHoldings()`
- `discoverTokens()`
- `queueTokenRefresh()`
- `queueCollectionRefresh()`

### Why this is required

All `/api/v1/*` routes require secrets and HMAC signing, so browser components must never call the platform directly.

### Signing rules the client must respect

The platform does more than check a signature, and two of these will break a naive client.

- **Every request is single-use.** Signatures are held in a replay guard for the accepted skew
  window. Replaying an identical signed request returns `409 replayed_request`. A retry must
  therefore re-sign with a fresh timestamp; retrying the same prepared request object always fails.
- **Clock skew is enforced.** Requests outside the configured window (300 seconds by default) are
  rejected with `401 stale_timestamp`. The marketplace host's clock must be in sync.
- **Rate limits are per API client, not per route.** Responses carry `x-ratelimit-limit` and
  `x-ratelimit-remaining`; exceeding the limit returns `429`. This matters because a single
  marketplace detail page under this plan makes several platform calls (token detail, owners,
  collection). Budget the limit against page views, and make the client surface remaining quota so
  throttling is observable.

### Implementation contract for the client

The client should expose a narrow interface like this:

```ts
export type PlatformDiscoveryStatus = 'ready' | 'queued' | 'failed';

export interface PlatformDiscoveryResult {
  chainId: number;
  contractAddress: string;
  tokenId: string;
  status: PlatformDiscoveryStatus;
  queuedJobId: string | null;
  jobId: string | null;
  /** Populated when status is `ready`, so an already-indexed NFT needs no follow-up read. */
  token: PlatformTokenSummary | null;
  collection: PlatformCollectionSummary | null;
  /** Populated when status is `failed`. */
  message?: string;
}

export interface NftDataPlatformClient {
  getTokenList(input: {
    chainId?: number;
    contractAddress?: string;
    metadataStatus?: PlatformMetadataState;
    mediaStatus?: PlatformMediaState;
    traitType?: string;
    traitValue?: string | number | boolean;
    limit?: number;
    cursor?: string;
  }): Promise<PlatformPage<PlatformTokenSummary>>;
  getTokenDetail(input: {
    chainId: number;
    contractAddress: string;
    tokenId: string;
  }): Promise<PlatformTokenDetail>;
  /** Ownership for one token. Required because token detail carries no owner. */
  getTokenOwners(input: {
    chainId: number;
    contractAddress: string;
    tokenId: string;
    limit?: number;
    cursor?: string;
  }): Promise<PlatformPage<PlatformTokenOwnership>>;
  listCollections(input: {
    limit?: number;
    cursor?: string;
  }): Promise<PlatformPage<PlatformCollectionSummary>>;
  getCollectionDetail(input: {
    chainId: number;
    contractAddress: string;
  }): Promise<PlatformCollectionSummary>;
  searchEntities(input: {
    query: string;
    entity?: 'tokens' | 'collections' | 'all';
    chainId?: number;
    contractAddress?: string;
    limit?: number;
    cursor?: string;
  }): Promise<PlatformPage<PlatformTokenSummary | PlatformCollectionSummary>>;
  getWalletInventory(input: {
    ownerAddress: string;
    chainIds?: number[];
    limit?: number;
    cursor?: string;
  }): Promise<PlatformPage<PlatformWalletHolding>>;
  discoverWalletHoldings(input: {
    ownerAddress: string;
    items: Array<{ chainId: number; contractAddress: string; tokenId: string }>;
  }): Promise<PlatformDiscoveryResult[]>;
  discoverTokens(input: {
    items: Array<{ chainId: number; contractAddress: string; tokenId: string }>;
  }): Promise<PlatformDiscoveryResult[]>;
  queueTokenRefresh(input: {
    chainId: number;
    contractAddress: string;
    tokenId: string;
    forceMetadata?: boolean;
    forceOwnership?: boolean;
  }): Promise<{ accepted: boolean; jobId?: string | null }>;
  queueCollectionRefresh(input: {
    chainId: number;
    contractAddress: string;
  }): Promise<{ accepted: boolean; jobId?: string | null }>;
}
```

This is the repo-level contract another developer should code against. If upstream DTOs change, only the client and its normalization layer should move.

### Platform constraints this interface reflects

These were verified against a running platform instance and are the reason the interface looks the
way it does. Getting them wrong produces code that appears to work and silently returns wrong data.

1. **Pagination is cursor-based, never page-based.** Every list and search endpoint returns
   `pageInfo.nextCursor` and accepts `?cursor=`. There is no `page` parameter.
2. **Unknown query parameters are ignored, not rejected.** `GET /api/v1/tokens?page=2&search=foo`
   answers `200` with an unfiltered first page. A client written against a `page`/`search`
   interface therefore looks healthy while always serving page one. This is the single most
   dangerous mismatch in this migration.
3. **Filters are single-valued.** `chainId` and `contractAddress` take one value each, not arrays.
   The multi-chain wallet endpoint is the exception: it accepts `chainIds=1,11155111` or a repeated
   `chainId` parameter.
4. **Free-text search lives only on `/api/v1/search`** via `q`. `/api/v1/tokens` filters
   structurally (chain, contract, status, trait) and has no text search.
5. **Token detail carries no ownership.** No `ownerAddress`, `owner`, `ownerBalance`, or `tokenUri`
   field exists on the token. Ownership comes from
   `GET /api/v1/owners/:chainId/:contractAddress/:tokenId`, and the metadata URI is
   `metadataUriResolved`.
6. **A collection list endpoint does exist**: `GET /api/v1/collections?limit=&cursor=`.

## 2. Add marketplace-owned proxy routes for the new platform

Browser code in this repository currently calls local routes such as `/api/wallet/nfts`, `/api/collections`, and `/api/nft/detail`.

That is a good pattern and should be kept.

### Required work

- keep browser-to-local-API access unchanged where practical
- replace internals of local routes so they proxy to the NFT Data Platform client instead of reading from local MongoDB for NFT data
- preserve the response shapes expected by the frontend until components are migrated cleanly

### Target route mapping

| Current local route | Current source | Target source |
| --- | --- | --- |
| `/api/wallet/nfts` | Alchemy/Moralis + MongoDB | NFT Data Platform wallet endpoint |
| `/api/user/nfts/sync` | Alchemy/Moralis + blockchain + MongoDB | NFT Data Platform discover endpoint |
| `/api/nft/detail` | MongoDB + blockchain refresh + IPFS | NFT Data Platform token detail |
| `/api/collections` | MongoDB aggregation | NFT Data Platform collection list or collection detail API |
| `/api/marketplace/items` | MongoDB materialized listings | stays TheGraph-backed or local read model backed by TheGraph |

### Route stability rule

During migration, keep the browser-facing route names stable and preserve the current high-level response envelopes.

| Local route | Keep stable | Allowed change |
| --- | --- | --- |
| `/api/wallet/nfts` | `success`, `data`, `total` envelope and current `WalletNFT` field names | internal source changes from legacy providers to NFT Data Platform |
| `/api/user/nfts/sync` | authenticated POST route and high-level sync result structure | internal discovery mechanism changes to NFT Data Platform discover job |
| `/api/nft/detail` | query params and `EnrichedNFTDocument`-oriented response structure | data source and indexing/error semantics |
| `/api/collections` | current collection card contract | collection source and preview-image sourcing |

## 3. Replace wallet discovery and wallet read flows

This is the highest-value migration area because wallet reads are currently the most coupled to Alchemy, Moralis, and local enrichment logic.

### Current coupling

- `src/contexts/wallet-nfts/WalletNFTsService.ts`
- `src/app/api/wallet/nfts/route.ts`
- `src/app/api/user/nfts/sync/route.ts`

### Required work

- make `/api/wallet/nfts` read from `GET /api/v1/owners/wallets/:ownerAddress`
- support multi-chain query forwarding using `chainIds` or repeated `chainId`
- map wallet inventory items into the current `WalletNFT` frontend shape
- preserve graceful fallback when `token` or `collection` is `null`
- make `/api/user/nfts/sync` call `POST /api/v1/owners/wallets/discover`
- stop using Alchemy and Moralis for primary wallet discovery in the marketplace app

### Concrete implementation contract for `/api/wallet/nfts`

Keep the current route path and return shape. Implement it in this order:

1. Validate `address` exactly as today.
2. Read optional `chainId` and `chainIds` query parameters and forward them to the platform client.
3. Call `client.getWalletInventory()`.
4. Join listing information from `marketplace_items` for matching `(chainId, contractAddress, tokenId)`.
5. Join local stats from `nft_stats` and local insights if the current wallet cards display them.
6. Map the result into the existing `WalletNFT[]` contract.
7. If the platform returns holdings with `token: null`, still return wallet entries with fallback identity and `hasMarketplaceData` derived only from local listing joins.

Recommended query compatibility rules:

- keep `skipPersist` temporarily, but repurpose it to mean `do not queue local side effects`
- treat `source` as deprecated input; do not let it choose Alchemy or Moralis once the feature flag is enabled
- it is acceptable to return `source: 'platform'` because current frontend usage is log-only, not behavioral

### Wallet mapping table

The mapper in `src/lib/nft-data-platform/mappers/wallet.ts` should implement the following defaults.

| `WalletNFT` field | Source |
| --- | --- |
| `contractAddress` | `holding.contractAddress.toLowerCase()` |
| `tokenId` | `holding.tokenId` |
| `name` | `holding.token?.name ?? 'NFT #' + holding.tokenId` |
| `description` | `holding.token?.description ?? null` |
| `image` | `holding.token?.imageUrl ?? null` |
| `animationUrl` | `holding.token?.animationUrl ?? null` |
| `attributes` | `holding.token?.attributes ?? []` |
| `contractName` | `holding.collection?.name ?? holding.token?.collection?.name ?? null` |
| `contractSymbol` | `holding.collection?.symbol ?? holding.token?.collection?.symbol ?? null` |
| `tokenType` | `holding.token?.standard ?? null` |
| `balance` | `holding.balance ?? (holding.token?.standard === 'ERC1155' ? '0' : '1')` |
| `owner` | `holding.ownerAddress` — the holding row, not the token; a token carries no owner |
| `ownerBalance` | `holding.balance` for ERC-1155, otherwise `null` |
| `tokenURI` | `holding.token?.metadataUri ?? null` |
| `isListed` and listing fields | joined from `marketplace_items` only |
| `stats` | joined from `nft_stats` only |
| `hasMarketplaceData` | `Boolean(listing)` |
| `hasInsightsData` | `Boolean(localInsights)` |

### Concrete implementation contract for `/api/user/nfts/sync`

The migrated sync route should stop acting like a full metadata fetch pipeline. Its job becomes orchestration:

1. Require authenticated wallet exactly as today.
2. Optionally inspect already-known local token identities for that wallet.
3. Call `client.discoverWalletHoldings()`.
4. Return a sync result that distinguishes `discover queued`, `already indexed`, and `upstream unavailable`.
5. Do not fetch token metadata from chain or IPFS directly in this route anymore.

Minimum expected response additions:

- `discoveryTriggered: boolean`
- `discoveryJobId?: string | null`
- `source: 'platform-discover' | 'legacy'`
- `indexingState: 'queued' | 'already-indexed' | 'unknown'`

### Important behavioral change

The NFT Data Platform wallet endpoint is ownership-first and not a universal live wallet crawler.

The frontend must therefore handle three valid states:

- holding exists and `token` is present
- holding exists but `token` is `null`
- holding is not returned yet because the platform has not discovered it

For the third case, the app should only rely on `discover` if another source already knows the wallet holdings.

## 4. Replace NFT detail reads with NFT Data Platform token detail

The current detail route still relies on local MongoDB plus on-demand blockchain sync.

### Current coupling

- `src/app/api/nft/detail/route.ts`
- `src/hooks/marketplace/useMarketplaceItemDetail.ts`
- `src/app/sell/components/forms/UnifiedListingForm.tsx`
- `src/contexts/CartContext.tsx`
- `src/app/cart/components/CartPage.tsx`

### Required work

- make `/api/nft/detail` fetch `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId`
- map NFT Data Platform response fields into the existing `EnrichedNFTDocument`-oriented frontend contract
- preserve current UI assumptions for metadata, media, attributes, owner, and collection context
- keep stats sourcing separate from token sourcing
- replace ad hoc refresh logic with `POST /api/v1/refresh/token` **only** when detail reads return `404`. Stale data no longer needs a marketplace-side trigger: reading a token that has aged past its TTL makes the platform queue its own refresh behind the response, debounced platform-side. The `freshness` block reports whether that happened.

### Concrete implementation contract for `/api/nft/detail`

Keep the route path and query parameters. Change the behavior as follows:

1. Resolve `chainId` deterministically. If the current UI does not supply it yet, derive it from the active marketplace listing or fall back to the marketplace default chain.
2. Call `client.getTokenDetail()` first.
3. Join listing state from `marketplace_items`.
4. Join stats from `nft_stats`.
5. Join admin insights from `admin_nft_insights`.
6. Fetch ownership with `client.getTokenOwners()` when the UI shows owner or balance. Token detail does not include it.
7. Map the normalized platform token into the existing `EnrichedNFTDocument`-oriented response contract.
8. Do not add a marketplace-side refresh cooldown for stale tokens. The platform already debounces
   its own revalidation, and a second cooldown here would only queue redundant jobs. Queue
   `client.queueTokenRefresh()` explicitly for the `404` case, where nothing is indexed yet and the
   platform has nothing to revalidate.

### Detail mapping table

The mapper in `src/lib/nft-data-platform/mappers/token-detail.ts` should apply these rules.

| Current detail field | Source |
| --- | --- |
| `contractAddress` | `token.contractAddress.toLowerCase()` |
| `tokenId` | `token.tokenId` |
| `metadata.name` | `token.name ?? 'NFT #' + token.tokenId` |
| `metadata.description` | `token.description ?? null` |
| `metadata.image` | `token.imageUrl ?? null` |
| `metadata.animationUrl` | `token.animationUrl ?? null` |
| `metadata.externalUrl` | `token.externalUrl ?? null` |
| `metadata.attributes` | `token.attributes ?? []` |
| `contract.owner` | separate `client.getTokenOwners()` call; token detail has no owner field |
| `contract.ownerBalance` | `balance` from the same ownership call, ERC-1155 only |
| `contract.tokenURI` | `token.metadataUri` (platform field `metadataUriResolved`) |
| `contract.name` | `token.collection?.name ?? null` |
| `contract.symbol` | `token.collection?.symbol ?? null` |
| `contract.contractType` | `token.standard ?? null` |
| `blockchain` | no direct chain read here; fill only from platform detail if the platform exposes that state, otherwise leave conservative nulls |
| `marketplace.*` | `marketplace_items` join only |
| `insights.*` | `admin_nft_insights` join only |
| `dataQuality.hasMetadata` | `token.metadataState === 'ok'` — the metadata enum is `pending`, `ok`, `failed`, `stale`; there is no `partial` state for metadata |
| `dataQuality.hasMedia` | `token.mediaState === 'ready' || token.mediaState === 'partial'` — the media enum is `pending`, `processing`, `ready`, `partial`, `failed` |
| `freshness.*` | `token.freshness` straight from the platform; do not recompute data age locally |
| `dataQuality.metadataSource` | `'cache'` when coming from indexed platform data, `'none'` when token metadata is absent |

### Required `404` behavior

When `client.getTokenDetail()` reports `404`, the local route should not silently pretend the token is absent forever.

Recommended response contract:

- HTTP `404`
- body code: `TOKEN_NOT_INDEXED`
- body fields: `refreshQueued`, `refreshJobId`, `contractAddress`, `tokenId`, `chainId`

`src/hooks/marketplace/useMarketplaceItemDetail.ts` must treat this as an indexing state and render placeholder data instead of a generic hard failure.

### Important limitation

The NFT Data Platform reads from its indexed MongoDB read model, not directly from chain state during the request.

That means UI code must stop assuming that missing data should be resolved synchronously by the detail endpoint.

## 5. Replace collection and search reads

Collections and search should come from the NFT Data Platform instead of local MongoDB aggregation where possible.

This applies to token and collection discovery data, not to local social stats. If the existing collections UI needs likes, views, watchlist counts, or ratings, those should still be joined from local MongoDB.

### Current coupling

- `src/app/api/collections/route.ts`
- `src/contexts/collections/CollectionsService.ts`

### Required work

- define whether the current collections page needs a list endpoint, a search endpoint, or a collection-detail lookup pattern
- if the page is contract-driven, proxy to `GET /api/v1/collections/:chainId/:contractAddress`
- if the page is browse-driven, proxy to `GET /api/v1/collections` (cursor-paginated) or `GET /api/v1/search?entity=collections`, and aggregate only the UI-specific view model locally
- preserve filters that are still needed by the existing collections UX

### Resolved: collection browsing exists

An earlier draft of this document treated collection browsing as an open gap. It is not: the
platform exposes `GET /api/v1/collections?limit=&cursor=`, cursor-paginated like every other list
endpoint. No local derived collections view and no temporary local aggregation is needed as a
workaround for a missing endpoint.

That does not change the decision below, which is a product choice rather than a technical
constraint: this marketplace shows *listed* collections, so the relevant contracts still come from
active listings. `listCollections()` is the right tool only if a global collection directory is
ever wanted.

### Implementation decision for this repository

For the current marketplace UI, this question is now resolved as follows:

1. `/api/collections` remains a listed-collections route, not a global browse route.
2. The set of relevant collection contracts continues to come from active `marketplace_items`.
3. Collection metadata must come from `client.getCollectionDetail()`.
4. Preview images do not need to be assembled from token summaries. A collection response already
   carries `preview` (the token the platform picked to represent the collection, with its media),
   `recentTokens` (most recently updated tokens with thumbnails), and `coverImageSource` telling
   you where the cover came from. It also carries `indexedTokenCount` and `holderCount`, both
   maintained by the platform's worker rather than counted per request.
5. Local social totals may still be aggregated locally if the UI displays them.

That means the migrated route should no longer treat `nft_metadata` as the primary metadata source for collections, even though the list of displayed contracts is still marketplace-driven.

## 6. Keep listing reads on TheGraph, but isolate them cleanly

TheGraph remains required for listing data.

### Current coupling

- `src/services/nft-sync/graph-subscription.ts`
- `src/app/api/marketplace/listing/[contractAddress]/[tokenId]/route.ts`
- `src/app/sell/success/page.tsx`
- `src/config/apolloClient.ts`
- `src/components/layout/Web3Provider.tsx`

### Required work

- keep TheGraph as the listing system of record
- reduce TheGraph responsibility to listing-only concerns
- stop using TheGraph or Apollo for token metadata, wallet inventory, or collection enrichment
- document that listing data and NFT data have different freshness guarantees

### Recommended rule

TheGraph should answer:

- is this token listed
- what is the listing price
- who is the seller
- what listing type is active
- what listing state is current

NFT Data Platform should answer:

- what the token is
- what media and metadata it has
- what collection it belongs to
- whether the token is indexed and enriched
- what a wallet owns

## 7. Build a merge layer for listing data plus NFT data

This is the most important architectural step for marketplace pages.

Marketplace cards and detail pages need one merged view composed from:

- **listing payload from TheGraph**
- **token and collection payload from NFT Data Platform**
- **stats and app-specific enrichment from local MongoDB**

### Required work

- introduce a dedicated mapper or composition service for merged marketplace view models
- merge by `chainId`, `contractAddress`, and `tokenId`
- make merged fields explicit instead of relying on MongoDB lookup side effects
- define deterministic fallbacks when listing exists but token enrichment is missing

### Required merge-layer responsibilities

Implement `src/services/marketplace/marketplace-view-composer.ts` or equivalent with explicit methods for:

- `composeMarketplaceCard()`
- `composeMarketplaceDetail()`
- `composeWalletNFT()`

Each method should accept three separate inputs:

1. listing data from TheGraph or `marketplace_items`
2. token or collection data from normalized NFT Data Platform DTOs
3. local stats and insights

Do not let UI-facing objects be built by ad hoc route-level object spreading anymore.

### Minimum merged card contract

- listing identity and status
- listing price and seller
- token name
- token media thumbnail or original image
- token standard
- collection name and symbol
- local stats such as views, likes, watchlist count, and rating when shown in the UI
- metadata and media state indicators

### Example fallback behavior

- listing exists, token exists: render normal card
- listing exists, token missing: render fallback card with token identity and loading/indexing state
- listing exists, collection missing: render card without collection decoration

## 8. Add configuration and environment support

The repository currently documents Alchemy, Moralis, and TheGraph env vars, but not NFT Data Platform credentials.

### Required work

- add server-only env vars for:
  - `NFT_API_BASE_URL`
  - `NFT_API_CLIENT_ID`
  - `NFT_API_KEY`
  - `NFT_API_SECRET`
- document that these must never be exposed via `NEXT_PUBLIC_*`
- add startup validation to fail fast when the platform integration is enabled but credentials are missing
- add a feature flag such as `NFT_DATA_PLATFORM_ENABLED=true`

### Exact rollout rule

Implement the rollout in both config and environment validation:

- add `NFT_DATA_PLATFORM_ENABLED` to environment handling
- add `FEATURES.NFT_DATA_PLATFORM_ENABLED` to `src/config/app.config.ts`
- update `scripts/check-env.js` so that when the feature flag is enabled, the following become required: `NFT_API_BASE_URL`, `NFT_API_CLIENT_ID`, `NFT_API_KEY`, `NFT_API_SECRET`

Required behavior:

- flag disabled: legacy code path may continue to run temporarily behind the same local routes
- flag enabled: wallet, detail, and collection primary reads must not call Alchemy or Moralis anymore
- secrets must remain server-only and never be mirrored into `NEXT_PUBLIC_*`

## 9. Update frontend assumptions and loading states

Several UI layers currently assume full enrichment is usually available immediately.

### Required work

- audit wallet cards, marketplace cards, detail pages, and sell flows for null-safe rendering
- render placeholders when `token` is `null`
- render collection fallback UI when `collection` is `null`
- handle `404` token detail responses as an indexing state, not always as a hard error
- expose refresh-queued states where useful

### Required UI state model

The wallet, marketplace, and detail UI should explicitly support these states:

| State | Expected UI behavior |
| --- | --- |
| listing present, token present | normal render |
| listing present, token missing | fallback card or detail shell with token identity and indexing message |
| holding present, token missing | wallet card with contract address, tokenId, and placeholder art |
| token present, collection missing | render token without collection decoration |
| token detail `404` with `TOKEN_NOT_INDEXED` | show indexing state, not generic error toast |
| refresh queued | show non-blocking informational state |

## 10. Decide what stays local to this app

Not everything should move into the NFT Data Platform.

These app-specific features can remain local unless you explicitly want to migrate them later:

- likes
- ratings
- watchlist
- personal notes
- cart state
- local UI cache invalidation
- admin insights stored only for this marketplace frontend

## Implementation Order

## PR-Ready Delivery Plan

This is the recommended change sequence for another developer implementing the migration.

| PR | Scope | Files touched at minimum | Acceptance check |
| --- | --- | --- | --- |
| 1 | foundation client | new `src/lib/nft-data-platform/*`, `src/config/app.config.ts`, `scripts/check-env.js`, docs | feature flag works and startup validation fails correctly when enabled without secrets |
| 2 | wallet read migration | `src/app/api/wallet/nfts/route.ts`, `src/lib/nft-data-platform/mappers/wallet.ts`, `src/contexts/wallet-nfts/WalletNFTsService.ts` | wallet route no longer calls Alchemy or Moralis when flag is enabled |
| 3 | wallet discover migration | `src/app/api/user/nfts/sync/route.ts` plus client methods | sync route triggers platform discovery and does not fetch metadata from chain/IPFS |
| 4 | token detail migration | `src/app/api/nft/detail/route.ts`, `src/lib/nft-data-platform/mappers/token-detail.ts`, `src/hooks/marketplace/useMarketplaceItemDetail.ts` | detail route reads platform first and handles `TOKEN_NOT_INDEXED` correctly |
| 5 | marketplace merge layer | new `src/services/marketplace/marketplace-view-composer.ts`, relevant marketplace route or hooks | marketplace card/detail payloads are composed from explicit sources |
| 6 | collections migration | `src/app/api/collections/route.ts`, `src/contexts/collections/CollectionsService.ts` | collections route no longer uses `nft_metadata` as primary metadata source |
| 7 | cleanup | remove legacy provider runtime dependencies from migrated paths, update tests and docs | migrated routes remain stable without Alchemy or Moralis in primary runtime reads |

## Phase 1: Foundation

1. Add the server-side NFT Data Platform client.
2. Add environment variables and validation.
3. Add a feature flag for controlled rollout.

## Phase 2: Wallet migration

1. Replace `/api/wallet/nfts` internals.
2. Replace `/api/user/nfts/sync` internals.
3. Keep the `WalletNFTsContext` public contract stable.

## Phase 3: NFT detail migration

1. Replace `/api/nft/detail` internals.
2. Update detail-page fallback logic.
3. Add refresh-job handling for `404` and incomplete metadata.

## Phase 4: Marketplace composition

1. Keep listing reads from TheGraph.
2. Add a merge layer for listing plus token data.
3. Update marketplace cards and detail view models.

## Phase 5: Collections and search

1. Replace `/api/collections` where possible.
2. Add NFT Data Platform backed search paths, using `q` on `/api/v1/search` rather than a `search`
   parameter on the token list.
3. Use the collection response's own `preview`, `recentTokens`, `indexedTokenCount`, and
   `holderCount` instead of rebuilding them from token queries.

## Phase 6: Cleanup

1. Remove Alchemy and Moralis from primary runtime reads.
2. Remove old wallet sync assumptions.
3. Reduce Apollo usage to listing-only code paths.

## Concrete File-Level Migration Checklist

### Open high priority

- `src/app/api/wallet/nfts/route.ts`
- `src/app/api/user/nfts/sync/route.ts`
- `src/app/api/nft/detail/route.ts`
- `src/contexts/wallet-nfts/WalletNFTsService.ts`
- `src/hooks/marketplace/useMarketplaceItemDetail.ts`

### Stats remain local

- `src/app/api/nft/stats/route.ts`
- `src/app/api/user/interactions/route.ts`
- `src/contexts/nft-stats/NFTStatsContext.tsx`
- `src/app/api/marketplace/items/route.ts` where `nft_stats` is joined for sorting and filtering

### Medium priority

- `src/app/api/collections/route.ts`
- `src/contexts/collections/CollectionsService.ts`
- `src/app/page.tsx`
- `src/app/marketplace/components/ListedNFTsList.tsx`
- `src/app/sell/components/forms/UnifiedListingForm.tsx`

### Listing-system isolation

- `src/services/nft-sync/graph-subscription.ts`
- `src/app/api/marketplace/listing/[contractAddress]/[tokenId]/route.ts`
- `src/app/sell/success/page.tsx`
- `src/config/apolloClient.ts`
- `src/components/layout/Web3Provider.tsx`

## Automated Test Plan To Add

Use route-level integration tests and mapper contract tests. At minimum add the following:

| Test file suggestion | Purpose |
| --- | --- |
| `tests/unit/nft-data-platform/wallet-mapper.test.ts` | normalized holding -> `WalletNFT` mapping including `token: null` |
| `tests/unit/nft-data-platform/pagination.test.ts` | cursor is passed through and a second page differs from the first, so a silently ignored parameter cannot pass |
| `tests/unit/nft-data-platform/signing.test.ts` | a retry re-signs with a fresh timestamp instead of replaying, and `409` is not treated as a permanent failure |
| `tests/unit/nft-data-platform/token-detail-mapper.test.ts` | normalized token detail -> current detail response mapping |
| `tests/integration/api/wallet-nfts.test.ts` | `/api/wallet/nfts` platform-backed happy path, `token: null`, listing join, stats join |
| `tests/integration/api/user-nfts-sync.test.ts` | `/api/user/nfts/sync` discover queued and upstream failure handling |
| `tests/integration/api/nft-detail.test.ts` | `/api/nft/detail` normal result, `TOKEN_NOT_INDEXED`, refresh queued |
| `tests/integration/api/collections.test.ts` | listed-collections derivation, collection hydration, preview image fallback |

## Testing Still Needed

The current repository has only limited test coverage for these flows.

Before cutting over production reads, add at least:

- contract tests for NFT Data Platform response mapping
- integration tests for `/api/wallet/nfts`
- integration tests for `/api/user/nfts/sync`
- integration tests for `/api/nft/detail`
- integration tests for merged marketplace cards when token or collection is missing
- tests for refresh-queue behavior after `404`

## Notable Risks

## 1. Wallet completeness risk

The NFT Data Platform wallet endpoint is not a universal live ownership crawler.

If the platform has not discovered a holding, the marketplace app will not see it unless another system provides those token identities and triggers discover.

## 2. Listing and NFT freshness mismatch

TheGraph listing data and NFT Data Platform token data may update on different timelines.

The UI must tolerate cases where:

- a listing is active before token enrichment is complete
- ownership changed but token enrichment has not caught up yet
- collection metadata lags behind listing visibility
- local stats exist even when token enrichment is still incomplete

## 3. Pagination and parameter mismatch

The platform ignores unknown query parameters instead of rejecting them. Code written against a
page-based or array-based interface returns `200` with plausible-looking data while silently
serving an unfiltered first page. Contract tests for the client layer should assert that a second
page differs from the first, not merely that a request succeeded.

## 4. Response-shape drift

The current frontend expects a custom `WalletNFT` and `EnrichedNFTDocument` shape. Directly piping platform payloads into the UI will likely break assumptions.

## Definition of Done

This migration is complete when all of the following are true:

- wallet reads no longer use Alchemy or Moralis at runtime
- token detail reads no longer use local MongoDB as the primary source
- collection and search reads are backed by the NFT Data Platform wherever endpoint support exists
- TheGraph is used only for listing data
- local MongoDB remains the source for nft_stats and user interaction aggregates
- marketplace cards are built by merging listing data with NFT Data Platform token and collection data
- the UI handles `token: null`, `collection: null`, and `404 not indexed yet` correctly
- NFT Data Platform credentials are fully server-side and validated at startup
- the migrated routes have automated coverage
- pagination uses platform cursors end to end, with no `page` parameter anywhere in the client
- owner and balance come from the ownership endpoint, never from token detail
- retries re-sign requests rather than replaying them, and `429` plus `409` are handled distinctly

## Recommended Next Step

Implement the migration in this order:

1. server-side NFT Data Platform client
2. `/api/wallet/nfts` migration
3. `/api/nft/detail` migration
4. merged marketplace card/detail mapper
5. collections and search migration

That sequence gives the best payoff while keeping listing behavior stable through TheGraph.