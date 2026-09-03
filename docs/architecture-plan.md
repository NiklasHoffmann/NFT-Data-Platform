# NFT Data Platform Architecture Plan

## Status

Approved on 2026-04-15 for implementation handoff.

Implementation notes updated on 2026-04-16:
- the operator UI now exposes distinct NFT, Collection, Jobs, Raw, and Operations views
- the operator UI now normalizes raw metadata fetch failures into operator-facing summaries while keeping source errors in persisted documents
- Operations queue health is derived from live BullMQ state, not only persisted Mongo job statuses
- the public media proxy is constrained to the configured object-storage public base path
- worker-side remote metadata and media fetches reject private, loopback, and link-local targets after hostname resolution
- signed API requests now combine timestamp freshness with Redis-backed one-time replay protection inside the accepted skew window
- the public health endpoint is intentionally minimal and no longer exposes runtime environment detail

Implementation notes updated on 2026-09-03:
- stored metadata now carries a TTL. A token read serves the stored state and queues a refresh behind the response when it has aged out, and the response reports that through a `freshness` block
- a worker sweep re-queues the oldest tokens and collections in bounded batches, so material that changes after it was first indexed no longer stays stale forever. Tokens in `failed` state use a separate, longer retry window
- collection counters (`indexedTokenCount`, `holderCount`) and the preview token are materialized by the worker instead of being recomputed on every read
- chain indexing stops short of the chain head by a configurable confirmation depth, because a reindex job for an already-indexed range is deduplicated by its idempotency key and would never repair a reorg
- the chain indexing and sweep loops hold Redis locks and read each chain head once per tick, so worker replicas do not multiply RPC cost or race on progress writes
- refresh queues process jobs in parallel; `reindex-range` stays serial because parallel range jobs on one collection would interleave ownership writes
- `audit_logs` and `jobs` have retention, applied through `collMod` so an index that already exists without a TTL is converted in place
- API clients are resolved from a short-lived cache and audit logging no longer blocks the response
- contract addresses in job payloads are validated and normalized, so one NFT maps to one job regardless of address casing

## Target stack

- Next.js + React for the read API and operator UI
- MongoDB for collections, tokens, ownership state, metadata versions, jobs, API clients, and audit logs
- Redis + BullMQ for refresh, reindex, metadata, and media jobs
- Separate Node.js worker for ingestion and asynchronous processing
- S3-compatible object storage for original media and derivatives
- CDN in front of object storage for media delivery

## Hard rules

- Never perform live on-chain reads in the request path for read APIs.
- Build read models and serve them fast from MongoDB and cache.
- Store media outside MongoDB.
- Run indexing, reindexing, metadata fetching, and media processing asynchronously.
- Treat ERC-721 and ERC-1155 ownership as separate state models.
- Protect every API request with API key auth, scopes, rate limits, and audit logging.
- Require HMAC request signing in the MVP for higher-trust integrations.

## Phase 1 implementation scope

1. Scaffold a monorepo with `apps/web`, `apps/worker`, and shared `packages/*` workspaces.
2. Set up TypeScript workspace defaults and local development scripts.
3. Add Docker Compose services for MongoDB, Redis, and MinIO.
4. Create the initial Next.js application shell for APIs and operator UI.
5. Create the initial worker shell for queue-backed processing.
6. Add shared packages for domain models, database access, queue contracts, chain helpers, storage, and security.

## Planned module boundaries

### apps/web

- Next.js app router
- read API routes
- operator-facing admin surface
- health endpoints
- future auth middleware and request auditing

### apps/worker

- BullMQ worker bootstrap
- metadata refresh processors
- media processing processors
- reindex and collection refresh processors
- future chain indexing loops

### packages/domain

- chain IDs
- NFT standards
- shared status enums
- token and collection contracts

### packages/db

- MongoDB client factory
- collection names
- future indexes and validators

### packages/queue

- queue names
- job payload schemas
- idempotency key helpers

### packages/chain

- chain registry
- URI normalization
- ERC-1155 token URI expansion

### packages/storage

- object storage configuration
- deterministic media key builders

### packages/security

- auth headers
- scopes
- API client contract
- HMAC canonicalization and verification helpers

## Early data model direction

- `collections`
- `tokens`
- `erc721_ownership`
- `erc1155_balances`
- `metadata_versions`
- `media_assets`
- `jobs`
- `api_clients`
- `audit_logs`

## Operational baseline

- Self-hosted local development via Docker Compose
- Initial supported chains: Ethereum Mainnet and Sepolia
- API-first architecture with asynchronous refresh orchestration
- Object storage bucket created locally as `nft-media`

## Milestones after Phase 1

Delivered:

1. MongoDB schemas, indexes, and validators.
2. API key + HMAC request verification backed by `api_clients`.
3. BullMQ queues and processors for refresh and reindex operations.
4. Collection registry and chain sync checkpoints.
5. Metadata versioning and media ingestion pipelines.
6. TTL-driven data freshness: read-path revalidation plus a worker sweep.
7. Materialized collection counters, so reads do not aggregate over ownership state.
8. Reorg-safe chain indexing and replica-safe background loops.
9. Retention on `audit_logs` and `jobs`.

Open:

1. Response caching for read endpoints. HMAC signing plus replay protection makes every request
   unique, so no CDN or proxy can reuse a response. Either read endpoints move to plain API-key
   auth so `ETag` and `Cache-Control` can work, or a server-side Redis read cache goes in front of
   the queries. This is a decision about the consumer auth contract, not just an optimization.
2. List endpoints still return the full `metadataPayload` per token, which makes large pages heavy.
   They need a projection and a slimmer list representation.
3. Search runs on non-indexable regular expressions over `name` and `description`, so it scans the
   collection. It needs a text index.
4. The worker job processors have no test coverage.
