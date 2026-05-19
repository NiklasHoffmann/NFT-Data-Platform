# NFT API quickstart

This note explains the shortest practical way to use the NFT Data Platform API for NFT data.

The built-in web frontend in this repository is only a test and operator surface. The intended production architecture is that separate wallet and marketplace services consume this API.

## Important behavior

The API reads from the indexed MongoDB read model, not directly from the blockchain during the request.

That means:

- reads are fast and stable
- a token can return `404` even if it exists on-chain but has not been indexed yet
- when data is missing or stale, trigger a refresh job first

## Authentication

All `/api/v1/*` routes require HMAC authentication.

Required headers:

- `x-client-id`
- `x-api-key`
- `x-signature`
- `x-timestamp`

The repository already includes a signed request helper:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/tokens?limit=10"
```

This command uses the bootstrap API client from `.env` and signs the request automatically.

## Most useful endpoints

### 1. List indexed NFTs

`GET /api/v1/tokens`

Use this for gallery pages, dashboards, backoffice exports, or general browsing.

Supported query parameters:

- `chainId`
- `contractAddress`
- `metadataStatus`
- `mediaStatus`
- `traitType`
- `traitValue`
- `limit` with max `200`
- `cursor`

Example:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/tokens?chainId=11155111&contractAddress=0xa7c41cea4f9195eebdc85054e6b0e799035bf02f&limit=20"
```

### 2. Read one NFT

`GET /api/v1/tokens/:chainId/:contractAddress/:tokenId`

Use this when you already know the token identity.

Example:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/tokens/11155111/0xa7c41cea4f9195eebdc85054e6b0e799035bf02f/4200042"
```

Successful responses return:

- `requestedIdentity`
- `lookup`
- `collection`
- `item`

If the token is not indexed yet, the route returns `404` with lookup metadata that tells you whether the collection is known.

### 2a. Sync external token lists

`POST /api/v1/tokens/discover`

Use this when another system such as TheGraph already knows which NFTs should appear in a frontend and you want the Data Platform to enrich them.

Behavior:

- already indexed NFTs are returned immediately as `discoveryStatus: ready`
- missing NFTs are queued as refresh jobs and returned as `discoveryStatus: queued`

Example:

```bash
npm run api:request -- POST "http://localhost:3000/api/v1/tokens/discover" '{"items":[{"chainId":11155111,"contractAddress":"0xa7c41cea4f9195eebdc85054e6b0e799035bf02f","tokenId":"4200042"}]}'
```

### 3. Read one collection

`GET /api/v1/collections/:chainId/:contractAddress`

Use this to fetch collection metadata and current indexing state.

Example:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/collections/11155111/0xa7c41cea4f9195eebdc85054e6b0e799035bf02f"
```

### 4. Search NFTs and collections

`GET /api/v1/search`

Useful query parameters:

- `q` required
- `entity=tokens|collections|all`
- `chainId`
- `contractAddress`
- `metadataStatus` for token search
- `mediaStatus` for token search
- `limit`
- `cursor`

Example:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/search?q=axie&entity=all&limit=10"
```

### 5. Read wallet inventory

For wallet or portfolio views, use one of these endpoints:

- `GET /api/v1/owners/wallets/:ownerAddress`
- `GET /api/v1/owners/wallets/:chainId/:ownerAddress`

These responses already embed token and collection summaries, so you usually do not need one extra token request per NFT.

Detailed wallet examples are in [docs/wallet-api.md](docs/wallet-api.md).

### 6. Sync external wallet holdings

`POST /api/v1/owners/wallets/discover`

Use this when another provider already knows the current wallet holdings and you want the Data Platform to do the enrichment.

Behavior:

- already indexed NFTs are returned immediately as `discoveryStatus: ready`
- missing NFTs are queued as refresh jobs and returned as `discoveryStatus: queued`

Example:

```bash
npm run api:request -- POST "http://localhost:3000/api/v1/owners/wallets/discover" '{"ownerAddress":"0x1234567890abcdef1234567890abcdef12345678","items":[{"chainId":11155111,"contractAddress":"0xa7c41cea4f9195eebdc85054e6b0e799035bf02f","tokenId":"4200042"}]}'
```

## Pagination

List and search endpoints use cursor pagination.

Pattern:

1. call the endpoint without `cursor`
2. read `pageInfo.nextCursor`
3. pass that value back as `?cursor=...`
4. stop when `pageInfo.hasMore` is `false`

## When data is missing

If a token or collection is not present yet, queue a refresh job.

Useful write endpoints:

- `POST /api/v1/refresh/token`
- `POST /api/v1/refresh/collection`

Example token refresh:

```bash
npm run api:request -- POST "http://localhost:3000/api/v1/refresh/token" '{"chainId":11155111,"contractAddress":"0xa7c41cea4f9195eebdc85054e6b0e799035bf02f","tokenId":"4200042","forceMetadata":true,"forceOwnership":true}'
```

The refresh call only queues work. Read the token again afterwards.

## Minimal integration flow

For most consumers, this is enough:

1. Call `GET /api/v1/tokens` for lists.
2. Call `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId` for detail pages.
3. Call `GET /api/v1/collections/:chainId/:contractAddress` for collection pages.
4. Call `GET /api/v1/search?q=...` for search.
5. If a token is missing, queue `POST /api/v1/refresh/token` and retry.

## Example response shape

Shortened token detail response:

```json
{
  "ok": true,
  "requestedIdentity": {
    "chainId": 11155111,
    "contractAddress": "0xa7c41cea4f9195eebdc85054e6b0e799035bf02f",
    "tokenId": "4200042"
  },
  "lookup": {
    "hasCollection": true,
    "hasToken": true
  },
  "collection": {
    "name": "Example Collection",
    "standard": "erc721",
    "syncStatus": "active"
  },
  "item": {
    "name": "Example NFT",
    "metadataStatus": "ok",
    "mediaStatus": "ready",
    "imageOriginalUrl": "https://...",
    "attributes": []
  }
}
```