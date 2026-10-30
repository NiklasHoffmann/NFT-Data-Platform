# The token response, field by field

What `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId` returns, and what a consuming
service — a marketplace, a wallet — needs to know about each part. Companion to
`docs/nft-api.md`, which covers the endpoints themselves.

Field names, types and status values below are taken from the serializers in
`packages/db/src/index.ts` and `apps/web/src/lib/token-response.ts`; the signing scheme from
`packages/security/src/index.ts`.

**About the example values.** The media block was measured against the running deployment on
30 October 2026: the object path, byte count, content type, checksum and response headers are
real. Everything else is a placeholder — `…` for strings, `0` for counters — because
plausible-looking invented metadata in a reference invites someone to code against it.

## The request

Every request is signed. Four headers, or the API answers `401 missing_auth_headers`.

| Header | Contents |
| --- | --- |
| `x-client-id` | The client id assigned when the API client was created |
| `x-api-key` | The client's public key |
| `x-signature` | HMAC-SHA256 over the canonical string, hex encoded |
| `x-timestamp` | Unix seconds, within `AUTH_MAX_TIMESTAMP_SKEW_SEC` (default 300) of server time |

### The canonical string

Four parts joined with `\n`. The path includes the query string, and the body is hashed even
when it is empty.

```
METHOD
/path?query
sha256hex(body)
timestamp
```

### Signing in Node

```js
import { createHash, createHmac } from "node:crypto";

const url = new URL("/api/v1/tokens/11155111/0x…/79", BASE_URL);
const body = "";                                  // GET: empty, but still hashed
const timestamp = Math.floor(Date.now() / 1000).toString();

const canonical = [
  "GET",
  `${url.pathname}${url.search}`,
  createHash("sha256").update(body).digest("hex"),
  timestamp
].join("\n");

const response = await fetch(url, {
  headers: {
    "x-client-id": CLIENT_ID,
    "x-api-key": API_KEY,
    "x-signature": createHmac("sha256", API_SECRET).update(canonical).digest("hex"),
    "x-timestamp": timestamp
  }
});
```

The secret belongs on a server. Signing in a browser would hand every visitor the ability to
call the API as the marketplace — which is also why the API itself needs no CORS allowance for
a server-to-server integration.

Required scope: `tokens:read`.

## The envelope

```jsonc
{
  "ok": true,
  "lookup": { "collectionStatus": "found", "tokenStatus": "found" },
  "requestedIdentity": { "chainId": 11155111, "contractAddress": "0x…", "tokenId": "79" },
  "freshness": { "…": "see below" },
  "item": { "…": "see below" },
  "collection": { "…": "see below" }
}
```

`lookup` reports what was resolved, independently of the HTTP status. On `404 token_not_found`
the `details` object still carries `lookup`, `requestedIdentity` and the `collection` when one
exists — so a consumer can tell "unknown contract" from "known contract, unknown token"
without a second request.

## item

Every stored field, plus two derived at read time: `creator` and `media`.

```jsonc
{
  "_id": "…",
  "chainId": 11155111,
  "contractAddress": "0x…",
  "tokenId": "79",
  "standard": "erc721",

  "name": "…",
  "description": "…",
  "externalUrl": null,
  "attributes": [{ "trait_type": "…", "value": "…", "display_type": "…" }],

  "metadataStatus": "ok",
  "metadataVersion": 0,
  "metadataHash": "…",
  "metadataUriRaw": "ipfs://…",
  "metadataUriResolved": "https://gateway.pinata.cloud/ipfs/…",
  "metadataPayload": { "…": "the raw metadata document" },
  "lastMetadataError": null,
  "lastMetadataFetchAt": "…",

  "imageOriginalUrl": "ipfs://…",
  "imageAssetId": "…",
  "animationOriginalUrl": null,
  "animationAssetId": null,
  "audioOriginalUrl": null,
  "audioAssetId": null,
  "interactiveOriginalUrl": null,
  "interactiveMediaType": null,

  "mediaStatus": "ready",
  "lastMediaProcessAt": "…",
  "supplyQuantity": null,
  "ownerStateVersion": 0,

  "createdAt": "…",
  "updatedAt": "…",

  "creator": { "name": "…", "address": "0x…", "source": "metadata.creator" },
  "media": { "…": "see below" }
}
```

### Status values worth branching on

| Field | Values | Meaning |
| --- | --- | --- |
| `metadataStatus` | `pending` · `ok` · `failed` · `stale` | Only `ok` and `stale` make `name`, `description` and `attributes` trustworthy. `stale` means it loaded once and has since aged past the TTL. |
| `mediaStatus` | `pending` · `processing` · `ready` · `partial` · `failed` | `ready` means every derivative is in the bucket. `partial` means at least one is missing, for example a video over the size limit. |
| `standard` | `erc721` · `erc1155` | `supplyQuantity` carries the amount for `erc1155` and is `null` for `erc721`. |
| `interactiveMediaType` | `html` · `youtube` · `interactive` | Set when the token's media is an interactive document rather than a file to mirror. |

### tokenId is a string

Always. Token ids routinely exceed `Number.MAX_SAFE_INTEGER` — one in the current data set has
77 digits. Parsing the response with a JSON reader that coerces numbers will silently corrupt
them.

### creator is a guess, not a record

`creator` is not stored. It is derived on every read by walking `metadataPayload` and matching
key names against:

```
/creator|artist|author|publisher|maker|brand|studio/i
```

On a hit, a string that is not a wallet address becomes `name`, one that is becomes `address`.
The first match in document order wins, arrays included. `source` is the path where it was
found, always prefixed with `metadata` — `metadata.creator`, `metadata.attributes[3].artist`,
`metadata.properties.studio.name`. No match gives `null`, which is common, because there is no
standard for authorship in NFT metadata.

Treat it as a display hint over arbitrary third-party JSON. For anything involving money, use
`collection.royaltyRecipientAddress` and `collection.royaltyBasisPoints`: those come from an
on-chain ERC-2981 `royaltyInfo` call, not from a pattern match.

## media

Three keys, each either the full asset document or `null`. This is the block a marketplace
renders.

```jsonc
"media": {
  "image": {
    "_id": "…",
    "tokenRef": "…",
    "kind": "image",
    "sourceUrl": "https://gateway.pinata.cloud/ipfs/…",

    "cdnUrlOriginal":  "https://s3.pfalz-development.de/nft-media/11155111/0x…/79/image/a0d0d4ab…/original",
    "cdnUrlOptimized": "https://s3.pfalz-development.de/nft-media/11155111/0x…/79/image/a0d0d4ab…/optimized",
    "cdnUrlThumbnail": "https://s3.pfalz-development.de/nft-media/11155111/0x…/79/image/a0d0d4ab…/thumbnail",

    "storageKeyOriginal":  "11155111/0x…/79/image/a0d0d4ab…/original",
    "storageKeyOptimized": "11155111/0x…/79/image/a0d0d4ab…/optimized",
    "storageKeyThumbnail": "11155111/0x…/79/image/a0d0d4ab…/thumbnail",

    "mimeType": "image/webp",
    "sizeBytes": 119684,
    "checksumSha256": "a0d0d4ab1916f61ef6bc4129fcc7c8d9263283d244deb8624eee0c5f10e2f6cc",
    "width": 0,          // actual pixel dimensions; not measured for this example
    "height": 0,
    "durationSec": null, // set for audio and video, null for images

    "status": "ready",
    "statusDetail": null,
    "createdAt": "…",
    "updatedAt": "…"
  },
  "animation": null,
  "audio": null
}
```

### Which URL for what

| Variant | Format | Use |
| --- | --- | --- |
| `cdnUrlThumbnail` | WebP, fits 400×400 | Grids, lists, preview tiles |
| `cdnUrlOptimized` | WebP, max 1600 px wide | Detail view — the default choice |
| `cdnUrlOriginal` | Source format | Download, zoom, formats sharp does not convert |

### Serving

Measured against the deployment:

```
HTTP/1.1 200 OK
Content-Type:  image/webp
Content-Length: 119684
Cache-Control: public, max-age=31536000, immutable
ETag: "87cf37d17c50b373bf091360856ba5a6"
```

Usable directly in `<img src>`; the bucket grants anonymous `s3:GetObject` and nothing else, so
object reads succeed while listing the bucket stays denied. The object key contains the file's
SHA-256, so a changed file means a changed path — a year of immutable caching cannot serve a
stale image.

CORS headers on the bucket are only needed if the consumer loads images through `fetch()` or a
canvas rather than `<img>`.

### Always check for null

`media.image` is `null` whenever no asset exists, which is the normal state while
`mediaStatus` is `pending` or `failed`. `item.imageOriginalUrl` is the fallback: it points at
the original source, is not size-bounded, and is not reliably reachable.

## freshness

The API answers from the read model and queues a refresh behind the response rather than making
the caller wait on chain and metadata IO.

| Field | Type | Meaning |
| --- | --- | --- |
| `lastMetadataFetchAt` | `string \| null` | ISO-8601 of the last metadata fetch |
| `ageSeconds` | `number \| null` | Age in seconds, `null` when never fetched |
| `isStale` | `boolean` | Past the TTL — `TOKEN_METADATA_TTL_SECONDS`, default 24 h, or `TOKEN_METADATA_FAILURE_RETRY_SECONDS`, default 6 h, after a failed fetch |
| `revalidationQueued` | `boolean` | This request queued a refresh |

`isStale: true` is not an error and not a reason to withhold the data. The values are correct,
they are simply older than the TTL, and the refresh is already running.

## collection

The same document `GET /api/v1/collections/:chainId/:contractAddress` returns. For a token
detail page usually only name, symbol and royalties matter.

Beyond the stored fields it carries three derived ones:

- `coverImageSource` — where the cover image came from: `collection-metadata`, `preview-token`,
  `recent-tokens` or `none`. Lets a consumer decide when to supply its own placeholder.
- `preview` — one representative token with its media, or `null`.
- `recentTokens` — recently updated tokens with their image asset, for collection tiles.

`indexedTokenCount` and `holderCount` are maintained by the worker rather than counted per
request, so they can trail the chain by one indexing cycle.

## What usually goes wrong

**A 401 with correct credentials** is almost always clock drift. The timestamp window is 300
seconds by default.

**Tokens without media are normal.** Some contracts expose no `tokenURI`; for others the
metadata source has disappeared. Then `name`, `attributes` and `media.*` are empty while the
token still exists on chain and still belongs to someone. A consumer needs a presentation for
that — hiding the token would misreport what a wallet holds.

**Use a dedicated API client with read scopes only:** `collections:read`, `tokens:read`,
`owners:read`, `search:read`. The bootstrap client additionally carries `refresh:*`,
`reindex:write` and `admin:read`, which would let a consuming service queue jobs and spend RPC
budget.

```bash
npm run api:create-client -- \
  --client-id marketplace \
  --scopes collections:read,tokens:read,owners:read,search:read \
  --rate-limit 600
```

The script writes to MongoDB directly, so it has to run where the database is reachable — the
app container's terminal, not a workstation. Credentials are printed once and the secret is
stored encrypted afterwards.
