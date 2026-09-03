# Wallet API integration

This note describes the recommended way to build an external wallet UI against the internal NFT Data Platform API.

## Recommended endpoint

For a wallet portfolio view, use the multi-chain wallet endpoint:

- `GET /api/v1/owners/wallets/:ownerAddress`

This route returns wallet holdings across supported chains and already embeds both token data and a compact collection summary per item.

The route is ownership-first:

- if ownership or balance is already materialized, the wallet item is returned
- if token metadata is not indexed yet, `token` can be `null`
- if the collection document is missing, `collection` can be `null`

The existing single-chain variant still exists:

- `GET /api/v1/owners/wallets/:chainId/:ownerAddress`

If your wallet application already knows the live wallet holdings from an external provider and wants the Data Platform to resolve missing NFTs, use:

- `POST /api/v1/owners/wallets/discover`

## Why this endpoint is the right fit

You usually do **not** need to:

- fetch a wallet inventory first,
- extract `contractAddress` and `tokenId`,
- then call `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId` for every NFT.

The wallet inventory response already includes:

- the holding identity: `chainId`, `contractAddress`, `tokenId`
- the ownership or balance document
- `token`: enriched NFT data
- `collection`: compact collection data

## Important limitation

The wallet endpoint does not do a full live on-chain wallet discovery across all NFT contracts.

It can only return holdings that are already known to the platform through indexed ownership or balance data.

That means:

- known holdings are returned even when token enrichment is still missing
- completely undiscovered collections or tokens are still not returned automatically
- for a guaranteed full wallet view across all NFT contracts, the platform would need an additional live wallet indexer or an external NFT ownership source

That makes it the best starting point for a wallet gallery or portfolio page.

## Query parameters

Supported query parameters for both wallet endpoints:

- `limit` — page size, max `200`, default `50`
- `cursor` — pagination cursor from the previous response
- `standard` — `erc721` or `erc1155`
- `contractAddress` — limit holdings to one collection
- `metadataStatus` — filter by token metadata state
- `mediaStatus` — filter by token media processing state
- `q` — text filter against token fields
- `traitType` — attribute name filter
- `traitValue` — attribute value filter

Additional query parameters for the multi-chain route:

- `chainIds=1,11155111`
- repeated `chainId`, for example `?chainId=1&chainId=11155111`

If no chain restriction is passed, the route searches all supported chains.

## Example requests

Trigger wallet discovery from externally resolved holdings:

```bash
npm run api:request -- POST "http://localhost:3000/api/v1/owners/wallets/discover" '{"ownerAddress":"0x1234567890abcdef1234567890abcdef12345678","items":[{"chainId":1,"contractAddress":"0xabc0000000000000000000000000000000000def","tokenId":"123"},{"chainId":11155111,"contractAddress":"0xdef0000000000000000000000000000000000abc","tokenId":"7"}]}'
```

This endpoint returns already indexed NFTs immediately and queues missing NFTs for token refresh.

Using the repository helper script:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/owners/wallets/0x1234567890abcdef1234567890abcdef12345678?limit=50"
```

Limit to specific chains:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/owners/wallets/0x1234567890abcdef1234567890abcdef12345678?chainIds=1,11155111&limit=50"
```

Filter to ERC-721 only:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/owners/wallets/0x1234567890abcdef1234567890abcdef12345678?standard=erc721&limit=50"
```

Single-chain variant:

```bash
npm run api:request -- GET "http://localhost:3000/api/v1/owners/wallets/1/0x1234567890abcdef1234567890abcdef12345678?limit=50"
```

## Example response shape

Wallet discover response shape:

```json
{
  "ok": true,
  "ownerAddress": "0x1234567890abcdef1234567890abcdef12345678",
  "summary": {
    "total": 2,
    "ready": 1,
    "queued": 1,
    "failed": 0
  },
  "items": [
    {
      "chainId": 1,
      "contractAddress": "0xabc0000000000000000000000000000000000def",
      "tokenId": "123",
      "ownerAddress": "0x1234567890abcdef1234567890abcdef12345678",
      "discoveryStatus": "ready",
      "queuedJobId": null,
      "jobId": null,
      "token": {},
      "collection": {}
    },
    {
      "chainId": 11155111,
      "contractAddress": "0xdef0000000000000000000000000000000000abc",
      "tokenId": "7",
      "ownerAddress": "0x1234567890abcdef1234567890abcdef12345678",
      "discoveryStatus": "queued",
      "queuedJobId": "refresh-token-...",
      "jobId": "6821a5a2d6d0e3f1a4d92ef1",
      "token": null,
      "collection": null
    }
  ]
}
```

Every nullable field in `token` and `collection` is always present in the response, using `null`
when there is no value. Fields are never omitted, so a strict client-side schema can rely on the
shape being stable.

Regular wallet inventory response shape:

```json
{
  "ok": true,
  "chainIds": [1, 11155111],
  "standard": "all",
  "items": [
    {
      "standard": "erc721",
      "_id": "6821a3f4d6d0e3f1a4d92e10",
      "chainId": 1,
      "contractAddress": "0xabc...def",
      "tokenId": "123",
      "ownerAddress": "0x123...678",
      "updatedAt": "2026-05-12T08:30:00.000Z",
      "token": {
        "_id": "6821a2d0d6d0e3f1a4d92dd0",
        "chainId": 1,
        "contractAddress": "0xabc...def",
        "tokenId": "123",
        "standard": "erc721",
        "name": "Example NFT",
        "description": "...",
        "metadataStatus": "ok",
        "mediaStatus": "ready",
        "imageOriginalUrl": "https://...",
        "animationOriginalUrl": null,
        "audioOriginalUrl": null,
        "interactiveOriginalUrl": null,
        "attributes": [],
        "media": {
          "image": {
            "_id": "6821a41dd6d0e3f1a4d92e88",
            "kind": "image",
            "cdnUrlOriginal": "https://...",
            "cdnUrlOptimized": "https://...",
            "cdnUrlThumbnail": "https://..."
          },
          "animation": null,
          "audio": null
        },
        "updatedAt": "2026-05-12T08:29:40.000Z"
      },
      "collection": {
        "_id": "6821a1d2d6d0e3f1a4d92d84",
        "chainId": 1,
        "contractAddress": "0xabc...def",
        "standard": "erc721",
        "name": "Example Collection",
        "symbol": "EX",
        "description": "...",
        "externalUrl": "https://...",
        "imageOriginalUrl": "https://...",
        "bannerImageOriginalUrl": "https://...",
        "featuredImageOriginalUrl": null,
        "animationOriginalUrl": null,
        "audioOriginalUrl": null,
        "interactiveOriginalUrl": null,
        "indexedTokenCount": 1000,
        "syncStatus": "active",
        "updatedAt": "2026-05-12T08:29:30.000Z"
      }
    },
    {
      "standard": "erc1155",
      "_id": "6821a5a2d6d0e3f1a4d92ef1",
      "chainId": 11155111,
      "contractAddress": "0x456...999",
      "tokenId": "7",
      "ownerAddress": "0x123...678",
      "balance": "3",
      "updatedAt": "2026-05-12T08:31:10.000Z",
      "token": {},
      "collection": {}
    }
  ],
  "pageInfo": {
    "limit": 50,
    "hasMore": true,
    "nextCursor": "eyJ1cGRhdGVkQXQiOiIyMDI2LTA1LTEyVDA4OjI5OjMwLjAwMFoiLCJpZCI6IjY4MjFhMWQyZDZkMGUzZjFhNGQ5MmQ4NCJ9"
  }
}
```

## Authentication model

All `/api/v1/*` routes are authenticated with:

- `x-client-id`
- `x-api-key`
- `x-signature`
- `x-timestamp`

The signature payload is:

```text
METHOD
/path?query=...
sha256(body)
timestamp
```

Do not call this API directly from a browser application. The secret must stay server-side.

## Example server-side client

The following helper can be dropped into another Node.js or Next.js project.

```ts
import { createHash, createHmac } from "node:crypto";

function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

function createWalletApiSignature(params: {
  method: string;
  pathWithQuery: string;
  body: string;
  timestamp: string;
  secret: string;
}): string {
  const payload = [
    params.method.toUpperCase(),
    params.pathWithQuery,
    sha256Hex(params.body),
    params.timestamp
  ].join("\n");

  return createHmac("sha256", params.secret).update(payload).digest("hex");
}

async function nftPlatformFetch<T>(pathWithQuery: string, init?: { method?: string; body?: unknown }): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase();
  const body = init?.body ? JSON.stringify(init.body) : "";
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const signature = createWalletApiSignature({
    method,
    pathWithQuery,
    body,
    timestamp,
    secret: process.env.NFT_API_SECRET!
  });

  const response = await fetch(`${process.env.NFT_API_BASE_URL}${pathWithQuery}`, {
    method,
    headers: {
      "content-type": "application/json",
      "x-client-id": process.env.NFT_API_CLIENT_ID!,
      "x-api-key": process.env.NFT_API_KEY!,
      "x-signature": signature,
      "x-timestamp": timestamp
    },
    body: method === "GET" || method === "HEAD" ? undefined : body
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`NFT API ${response.status}: ${responseText}`);
  }

  return JSON.parse(responseText) as T;
}

export async function getWalletInventory(params: {
  ownerAddress: string;
  chainIds?: number[];
  standard?: "erc721" | "erc1155";
  limit?: number;
  cursor?: string;
}) {
  const searchParams = new URLSearchParams();

  if (params.limit) {
    searchParams.set("limit", String(params.limit));
  }

  if (params.standard) {
    searchParams.set("standard", params.standard);
  }

  if (params.cursor) {
    searchParams.set("cursor", params.cursor);
  }

  for (const chainId of params.chainIds ?? []) {
    searchParams.append("chainId", String(chainId));
  }

  const query = searchParams.toString();
  const path = `/api/v1/owners/wallets/${params.ownerAddress}${query ? `?${query}` : ""}`;

  return nftPlatformFetch(path);
}
```

## Suggested integration flow for another project

1. Call the multi-chain wallet endpoint from your server.
2. Render `items[].token` directly for NFT cards.
3. Use `items[].collection` for collection name, symbol, and visual context.
4. Use `pageInfo.nextCursor` for infinite scroll or pagination. Treat it as an opaque string: it is a base64url-encoded payload and its internal format is not part of the API contract.
5. Fall back gracefully when `token` or `collection` is `null`, because holdings can exist before all metadata is fully materialized.