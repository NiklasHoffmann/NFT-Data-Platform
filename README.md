# NFT Data Platform

NFT Data Platform is a TypeScript monorepo for ingesting, normalizing, and serving NFT collection and token data across ERC-721 and ERC-1155 contracts. It combines a protected read API, an operator-facing discovery UI, a BullMQ-backed worker pipeline, and S3-compatible media storage. The repository is best understood as a practical portfolio project focused on asynchronous ingest architecture, read-model design, and operational concerns around self-hosted blockchain data systems.

## What this repository demonstrates

- A clear split between request-time reads and asynchronous blockchain ingestion.
- A monorepo with a Next.js application, a separate worker, and shared workspace packages for domain, queue, storage, and security concerns.
- Protected API surfaces using API keys, HMAC request signing, scope checks, IP allowlists, replay protection, rate limiting, and audit logging.
- Read models for collections, tokens, ERC-721 ownership, ERC-1155 balances, metadata versions, media assets, jobs, API clients, and audit logs.
- Operator tooling for inspecting the current indexed state rather than reading live chain data in the request path.
- Deployment thinking for local Docker Compose and a first-pass Coolify deployment with MongoDB auth, Redis auth, and internal MinIO.

## Tech stack

- Next.js 15 + React 19 for the API and operator UI.
- TypeScript across the full monorepo.
- MongoDB for normalized read models and operational records.
- Redis + BullMQ for refresh, media, and reindex workflows.
- MinIO / S3-compatible object storage for mirrored media.
- Zod for runtime validation of environment variables, job payloads, and request inputs.
- AWS SDK S3 client for storage-backed media reads through the web application.

## System overview

### apps/web

- Next.js App Router application.
- Serves the operator-facing discover UI at the root route.
- Exposes HMAC-protected read and mutation endpoints under `/api/v1/*`.
- Proxies browser-safe media reads through `/api/media`.
- Exposes a minimal health route at `/api/health`.

### apps/worker

- BullMQ worker process for queued background work.
- Handles collection refresh, token refresh, media refresh, and reindex-range jobs.
- Runs optional background chain indexing for active collections.
- Writes normalized state back into MongoDB instead of reading on-chain during API requests.

### packages/*

- `domain`: shared enums, schemas, and blockchain-facing data contracts.
- `db`: Mongo client, validators, indexes, and read/write helpers.
- `queue`: queue names, payload schemas, and queue option helpers.
- `chain`: contract reads, URI normalization, deployment checks, and transfer/indexing helpers.
- `storage`: object-storage configuration and deterministic media key generation.
- `security`: API auth, HMAC signing/verification, scopes, and bootstrap client helpers.
- `runtime`: shared environment loading utilities.

## Product areas

### Operator discover surface

The operator UI is not a generic landing page. It is an inspection surface for the current read model. The main discover flow queues refresh work, waits for queued jobs to settle, and renders whatever state is already materialized in MongoDB.

Current views include:

- `NFT`: token-centric media, ownership, attributes, metadata payloads, and current lookup state.
- `Collection`: collection metadata, contract signals, and indexed token coverage.
- `Jobs`: related queue-backed job records.
- `Raw`: stored MongoDB documents as currently materialized.
- `Operations`: live BullMQ-backed queue health and indexing lag.

### Protected read API

The `/api/v1/*` surface is designed as an internal or higher-trust integration API rather than an anonymous public REST API. Requests are authenticated through API key headers and HMAC signatures, then checked for scopes, IP policy, replay safety, and rate limits before the handler runs.

### Media ingestion and delivery

Token and collection media are mirrored into object storage when possible. The application then serves browser-safe previews through `/api/media`, restricted to the configured storage namespace. Oversized or unsupported assets can remain external fallbacks while still appearing in the operator UI.

### Background indexing

The worker supports both targeted refresh jobs and optional ongoing chain indexing. Collection documents track observed and indexed checkpoints so the system can enqueue bounded `reindex-range` jobs instead of repeatedly scanning entire chains.

## Why the project is technically interesting

- It avoids live chain reads in the request path and treats indexing as an asynchronous systems problem.
- It supports both ERC-721 and ERC-1155 with different ownership materialization models.
- It keeps raw source errors while also surfacing operator-friendly failure summaries in the UI.
- It handles media as a separate storage pipeline rather than embedding blobs in MongoDB.
- It includes operational plumbing that many sample projects skip: queue state, replay protection, audit logging, deployment configuration, and smoke scripts.

## Local development

### Prerequisites

- Node.js 20+
- Docker Desktop or another Docker runtime

### Setup

1. Copy `.env.example` to `.env`.
2. Fill in RPC URLs and any local overrides you need.
3. Install dependencies with `npm install`.
4. Start infrastructure with `docker compose up -d`.
5. Initialize MongoDB validators, indexes, and the bootstrap API client with `npm run db:init`.
6. Start the web app with `npm run dev:web`.
7. Start the worker with `npm run dev:worker`.

### Local infrastructure

`docker-compose.yml` starts:

- MongoDB 8
- Redis 7
- MinIO
- a MinIO init container that creates the `nft-media` bucket for local use

## Environment variables

The project uses `.env.example` as the local baseline and validates web runtime configuration in `apps/web/src/lib/env.ts`.

### Required to run the stack meaningfully

- `APP_BASE_URL`
- `MONGODB_URI`
- `MONGODB_DATABASE`
- `REDIS_URL`
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_BUCKET`
- `S3_PUBLIC_BASE_URL`
- `API_CLIENT_SECRET_ENCRYPTION_KEY`

### Required for chain-backed ingest

- `RPC_MAINNET_URL`
- `RPC_SEPOLIA_URL`

The example file also includes websocket variants, but the current worker bootstrap shown in this repository uses the HTTP RPC URLs.

### Bootstrap API client

- `API_BOOTSTRAP_CLIENT_ID`
- `API_BOOTSTRAP_KEY`
- `API_BOOTSTRAP_SECRET`
- `API_BOOTSTRAP_SCOPES`
- `API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE`
- `API_BOOTSTRAP_ALLOWED_IPS`
- `AUTH_MAX_TIMESTAMP_SKEW_SEC`

### Optional worker behavior

- `MEDIA_MAX_VIDEO_BYTES`
- `CHAIN_INDEXING_ENABLED`
- `CHAIN_INDEXING_POLL_INTERVAL_MS`
- `CHAIN_INDEXING_BATCH_SIZE`
- `CHAIN_INDEXING_MAX_BLOCK_RANGE`
- `CHAIN_INDEXING_COLLECTION_ALLOWLIST`

## Representative routes

### Operator UI

- `/` — discover and inspect indexed collection/token state
- `/?view=collection` — collection-focused operator view
- `/?view=jobs` — related queued job history
- `/?view=raw` — raw Mongo-backed document inspection
- `/?view=operations` — queue activity and indexing lag

### Utility routes

- `GET /api/health` — minimal readiness signal
- `GET /api/media?url=...` — storage-constrained media proxy for previews

## Representative APIs

All `/api/v1/*` routes are protected by HMAC-based API authentication.

- `GET /api/v1/tokens` — paginated token listing with filters for chain, contract, metadata status, media status, and traits
- `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId` — token read with lookup state and optional collection context
- `GET /api/v1/collections/:chainId/:contractAddress` — collection read with requested identity and lookup metadata
- `GET /api/v1/search` — token, collection, or mixed search across indexed data
- `GET /api/v1/owners/:chainId/:contractAddress/:tokenId` — ownership view for a token
- `GET /api/v1/owners/wallets/:chainId/:ownerAddress` — mixed ERC-721 / ERC-1155 wallet inventory
- `POST /api/v1/refresh/token` — queue token refresh work
- `POST /api/v1/refresh/collection` — queue collection refresh work
- `POST /api/v1/refresh/media` — queue media refresh work
- `POST /api/v1/reindex` — queue bounded reindex work

## Scripts

### Root workspace

- `npm run dev:web`
- `npm run dev:worker`
- `npm run build`
- `npm run lint`
- `npm run typecheck`
- `npm run db:init`
- `npm run api:request`

### Smoke and regression scripts

- `npm run smoke:fixtures` — validates known reference fixtures against the API
- `npm run smoke:refresh` — exercises queued refresh flows end to end
- `npm run smoke:discover-regressions` — runs targeted regression cases for known edge collections and tokens
- `npm run smoke:reindex-erc721` — verifies ERC-721 ownership reconstruction through reindexing

These scripts are part of what makes the repository useful for technical review: the project includes evidence of validation beyond manual browsing.

## Deployment

### Local

Use `docker-compose.yml` for MongoDB, Redis, and MinIO, then run the web and worker processes from the workspace.

### Coolify

`docker-compose.coolify.yml` defines a first-pass self-hosted deployment with:

- public `web`
- internal `worker`
- authenticated `mongo`
- password-protected `redis`
- internal `minio` and `minio-init`

Notes based on the current codebase:

- The web service listens on port `3000`.
- `S3_PUBLIC_BASE_URL` can stay on the internal MinIO URL because the web app reads storage objects through credentials and re-serves them through `/api/media`.
- MongoDB and Redis credentials should be URL-safe because they are interpolated into connection URIs.
- `CHAIN_INDEXING_ENABLED=false` is a reasonable initial deployment default.

## Code quality and engineering signals

- Environment variables are validated with Zod.
- API inputs and job payloads are schema-validated.
- MongoDB collections and indexes are bootstrapped through `npm run db:init`.
- Authenticated routes write audit logs and enforce replay/rate-limit checks.
- The worker distinguishes retryable queue failures from terminal failures.
- Media fetches reject internal network targets and constrain proxying to known storage paths.

## Additional documentation

- `docs/architecture-plan.md` — architecture baseline and implementation direction
- `docker-compose.coolify.yml` — production-oriented deployment scaffold
- `scripts/` — operational and regression tooling

## Repository framing

This repository is not a generic NFT viewer and not a framework starter. Its value is in the engineering tradeoffs it exposes: how to move blockchain and media work out of request handlers, how to model read state for multiple NFT standards, how to protect internal APIs, and how to make the resulting system inspectable and operable.
