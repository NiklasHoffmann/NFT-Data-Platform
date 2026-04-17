# NFT Data Platform

Greenfield monorepo for an NFT data platform targeting ERC-721 and ERC-1155 ingestion, normalized read models, protected APIs, async refresh workflows, and object-storage-backed media delivery.

## Workspace layout

- `apps/web`: Next.js application for API and operator UI.
- `apps/worker`: Node.js worker for indexing, refresh jobs, metadata resolution, and media processing.
- `packages/*`: Shared domain, database, queue, chain, storage, and security building blocks.
- `docs/architecture-plan.md`: Approved implementation plan and architecture baseline.

## Local development

1. Copy `.env.example` to `.env` and fill in RPC credentials.
2. Install workspace dependencies with `npm install`.
3. Start infrastructure with Docker Compose.
4. Initialize MongoDB indices and seed the bootstrap API client with `npm run db:init`.
5. Start the web app with `npm run dev:web`.
6. Start the worker with `npm run dev:worker`.

Runtime notes:
- `S3_PUBLIC_BASE_URL` is the public media base URL written into stored assets. When it points at local MinIO, the web app can proxy those URLs back through the app origin for browser-safe previews.
- The `/api/media` proxy now forwards only assets under the configured `S3_PUBLIC_BASE_URL` origin and path prefix. It no longer accepts arbitrary loopback targets.
- `MEDIA_MAX_VIDEO_BYTES` caps how much video the worker mirrors into object storage. Oversized animation or video stays available as external fallback and can leave a token in partial media state.
- Persisted metadata and media URL fields are validated against the URI formats the pipeline actually supports: `http(s)`, normalized `ipfs`/`ar` references, and `data:` URLs where inline media is expected.
- Worker-side metadata and media fetches reject loopback, private, link-local, multicast, and other internal network targets after hostname resolution, not only by raw hostname string.
- Media refresh is retry-aware: temporary gateway, timeout, and network failures stay retryable instead of being finalized immediately, and IPFS-style media fetches try multiple public gateways before the job gives up.
- `CHAIN_INDEXING_ENABLED=true` turns on the worker-side auto-index loop for active ERC-721 and ERC-1155 collections. It polls active collections, compares `lastIndexedBlock` against the observed chain head, and enqueues idempotent `reindex-range` jobs in bounded block windows.
- `CHAIN_INDEXING_COLLECTION_ALLOWLIST` can locally narrow that loop to a comma-separated set of `<chainId>:<contractAddress>` identities so development runs do not fan out across every active collection in Mongo.
- The local `.env` in this workspace now enables chain indexing, so restarting the worker is enough to pick the loop up.

## Operator UI

- The discover surface is split into five views: `NFT`, `Collection`, `Jobs`, `Raw`, and `Operations`.
- `NFT` stays token-centric. It keeps the collection address for identity, but collection owner, royalty, and contract-URI signals live in the dedicated `Collection` tab.
- `Collection` separates metadata-derived fields from contract-derived signals. Contract URI transport details, owner, and royalty signals live under `Contract signals`.
- Collection holder summaries can intentionally show `not indexed yet` or `snapshot unavailable` when ownership state has not been materialized into MongoDB yet.
- Metadata fetch failures are shown in the operator UI as normalized operator-facing summaries, while the underlying stored documents still retain the raw source error strings.
- `Operations` now shows live BullMQ queue activity rather than any MongoDB job document that still happens to say `queued` or `running`.

## Bootstrap auth

- `API_CLIENT_SECRET_ENCRYPTION_KEY` must be a 32-byte key encoded as 64 hex chars or base64.
- `npm run db:init` creates the core MongoDB collection validators and indexes and upserts the bootstrap API client into `api_clients`.
- Until `db:init` has run successfully, the API can still authenticate with the bootstrap env credentials as a fallback.
- Signed API requests are freshness-checked with `x-timestamp` and are treated as one-time requests within the accepted replay window via Redis-backed replay protection.

## Public endpoints

- `GET /api/health` is intentionally minimal and returns only basic service readiness metadata: `ok`, `service`, and `status`.
- `GET /api/media?url=...` is unauthenticated for browser previews, but it is restricted to the configured media base URL namespace.

## Response notes

- `GET /api/v1/collections/:chainId/:contractAddress` includes `lookup` and `requestedIdentity` context on success and on `collection_not_found` responses so the operator surface can distinguish lookup state from missing data.

## Search API

- `GET /api/v1/search` supports `entity=tokens`, `entity=collections`, and `entity=all`.
- `entity=all` returns a mixed feed sorted by `updatedAt` descending and includes an `entity` field on each item.
- `metadataStatus` and `mediaStatus` are token-only filters and are accepted only when `entity=tokens`.

## Owner API

- `GET /api/v1/owners/:chainId/:contractAddress/:tokenId` returns token ownership for both standards: a single ERC-721 owner record or paginated ERC-1155 holder balances.
- `GET /api/v1/owners/wallets/:chainId/:ownerAddress` returns a mixed paginated wallet inventory across ERC-721 and ERC-1155 and embeds matching token data.
- Wallet inventory supports `standard`, `contractAddress`, `q`, `metadataStatus`, `mediaStatus`, `traitType`, and `traitValue` to filter embedded token matches before pagination.
- ERC-1155 holder state is materialized during `reindex-range` jobs into `erc1155_balances` and then served from MongoDB.
- Collection refresh now persists `deployBlock`, and ERC-1155 reindex replays from that block to avoid full-chain scans on public RPC endpoints.
- Collection documents now distinguish `lastIndexedBlock` from the observed chain head so continuous background indexing can enqueue only the missing ranges.

## Smoke Checks

- `npm run smoke:fixtures` validates the current local API against the two canonical reference cases used during development:
- ERC-1155 `My Happy Tent` on Sepolia with token `4`
- ERC-721 `People of History - Bolivar` on Sepolia with token `359`
- The script is read-only and assumes the API is running locally and the fixtures were already refreshed into MongoDB.
- `npm run smoke:refresh` validates the queued refresh flow for the same fixtures by posting refresh jobs, waiting for Mongo job state to reach `done`, and re-reading the resulting API state.
- `smoke:refresh` requires both the web app and worker to be running locally.
- `npm run smoke:discover-regressions` exercises the current regression matrix for known edge-case collections and tokens, including partial-media and ERC-1155 fallback cases.
- `npm run smoke:reindex-erc721` deletes the local ERC-721 ownership snapshot for a bounded Panini mainnet fixture, runs a `reindex-range` processor pass, and verifies that the owner record is rebuilt from chain history.
