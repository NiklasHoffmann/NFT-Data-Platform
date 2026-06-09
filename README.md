# NFT Data Platform

NFT Data Platform is a TypeScript monorepo for ingesting, normalizing, and serving NFT collection and token data across ERC-721 and ERC-1155 contracts. It combines a protected read API, a temporary operator-facing discovery UI for testing, a BullMQ-backed worker pipeline, and S3-compatible media storage. The long-term architecture treats this repository primarily as an API and worker service that will be consumed by separately developed wallet, marketplace, and admin-governance services.

## What this repository demonstrates

- A clear split between request-time reads and asynchronous blockchain ingestion.
- A monorepo with a Next.js API/test surface, a separate worker, and shared workspace packages for domain, queue, storage, and security concerns.
- Protected API surfaces using API keys, HMAC request signing, scope checks, IP allowlists, replay protection, rate limiting, and audit logging.
- Read models for collections, tokens, ERC-721 ownership, ERC-1155 balances, metadata versions, media assets, jobs, API clients, and audit logs.
- Operator tooling for inspecting the current indexed state rather than reading live chain data in the request path.
- Deployment thinking for local Docker Compose and an app-only Coolify deployment where MongoDB, Redis, and S3/MinIO are managed outside the app stack.

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
- Serves the API surface and a temporary operator-facing discover UI for testing and inspection.
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

The operator UI is not a generic landing page and not intended as the long-term user-facing product frontend. It is an internal inspection and test surface for the current read model. The main discover flow queues refresh work, waits for queued jobs to settle, and renders whatever state is already materialized in MongoDB.

Current views include:

- `NFT`: token-centric media, ownership, attributes, metadata payloads, and current lookup state.
- `Collection`: collection metadata, contract signals, and indexed token coverage.
- `Jobs`: related queue-backed job records.
- `Raw`: stored MongoDB documents as currently materialized.
- `Operations`: live BullMQ-backed queue health and indexing lag.

### Protected read API

The `/api/v1/*` surface is designed as an internal or higher-trust integration API rather than an anonymous public REST API. Requests are authenticated through API key headers and HMAC signatures, then checked for scopes, IP policy, replay safety, and rate limits before the handler runs.

This API is intended to be consumed by separately deployed wallet, marketplace, and admin-governance services in a microservices-style architecture.

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
For app-only Coolify deployments with external dependencies, use `.env.coolify.example` as the baseline.

### Required to run the stack meaningfully

- `APP_BASE_URL`
- `MONGODB_URI`
- `MONGODB_DATABASE`
- `REDIS_URL`
- `CORS_ALLOWED_ORIGINS`
- `API_MAX_REQUEST_BYTES`
- `PUBLIC_READ_RATE_LIMIT_PER_MINUTE`
- `S3_ENDPOINT`
- `S3_REGION`
- `S3_ACCESS_KEY`
- `S3_SECRET_KEY`
- `S3_BUCKET`
- `S3_PUBLIC_BASE_URL`
- `API_CLIENT_SECRET_ENCRYPTION_KEY`

### Required for chain-backed ingest

- `RPC_URL_<chainId>` entries for chains you ingest from (for example `RPC_URL_1`, `RPC_URL_11155111`)

Legacy `RPC_MAINNET_URL` and `RPC_SEPOLIA_URL` names are still accepted in code as a fallback, but `RPC_URL_<chainId>` is the recommended format.

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

- `GET /api/health` — aggregated readiness signal for web + dependencies
- `GET /api/health/db` — MongoDB dependency status
- `GET /api/health/redis` — Redis dependency status
- `GET /api/health/storage` — S3/MinIO dependency status
- `GET /api/media?url=...` — storage-constrained media proxy for previews

## Representative APIs

All `/api/v1/*` routes are protected by HMAC-based API authentication.

- `GET /api/v1/tokens` — paginated token listing with filters for chain, contract, metadata status, media status, and traits
- `GET /api/v1/tokens/:chainId/:contractAddress/:tokenId` — token read with lookup state and optional collection context
- `GET /api/v1/collections/:chainId/:contractAddress` — collection read with requested identity and lookup metadata
- `GET /api/v1/search` — token, collection, or mixed search across indexed data
- `GET /api/v1/owners/:chainId/:contractAddress/:tokenId` — ownership view for a token
- `GET /api/v1/owners/wallets/:chainId/:ownerAddress` — mixed ERC-721 / ERC-1155 wallet inventory
- `GET /api/v1/owners/wallets/:ownerAddress` — multi-chain wallet inventory across supported chains with embedded token and collection summaries
- `POST /api/v1/owners/wallets/discover` — accepts externally resolved wallet holdings, returns indexed NFTs immediately, and queues missing NFTs for discovery
- `POST /api/v1/tokens/discover` — accepts externally resolved token identities, returns indexed NFTs immediately, and queues missing NFTs for discovery
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

`docker-compose.coolify.yml` defines an app-only deployment with:

- public `web`
- internal `worker`

This setup keeps deploys fast by restarting only application containers. Data services stay online in separate managed services (or separate Coolify resources).

Notes based on the current codebase:

- The web service listens on port `3000`.
- In Coolify Compose deployments, if the `web` service listens on container port `3000`, enter the domain with `:3000` in Coolify for that service so the proxy routes to the correct internal port while still exposing the app on the normal public URL.
- `MONGODB_URI`, `REDIS_URL`, `S3_ENDPOINT`, and `S3_PUBLIC_BASE_URL` must point to externally reachable services from inside Coolify.
- `CHAIN_INDEXING_ENABLED=false` is a reasonable initial deployment default.
- After first successful deploy, run `npm run db:init` once against production env to ensure validators, indexes, and bootstrap API client records are in place.

For exact UI-level setup fields and ordering, follow `docs/coolify-step-by-step.md`.

## Code quality and engineering signals

- Environment variables are validated with Zod.
- API inputs and job payloads are schema-validated.
- MongoDB collections and indexes are bootstrapped through `npm run db:init`.
- Authenticated routes write audit logs and enforce replay/rate-limit checks.
- The worker distinguishes retryable queue failures from terminal failures.
- Media fetches reject internal network targets and constrain proxying to known storage paths.

## Additional documentation

- `docs/architecture-plan.md` — architecture baseline and implementation direction
- `docs/nft-api.md` — quickstart for the protected NFT API including list, detail, search, and discover flows
- `docs/wallet-api.md` — wallet inventory integration guide with the multi-chain endpoint, example response shape, and a server-side HMAC client snippet
- `docs/wallet-application-requirements.md` — target architecture and implementation requirements for the separate wallet service
- `docs/marketplace-application-requirements.md` — target architecture and implementation requirements for the separate marketplace service
- `docs/admin-governance-application-requirements.md` — target architecture and implementation requirements for the separate admin and governance service
- `docs/coolify-step-by-step.md` — exact Coolify setup for MongoDB, Redis, MinIO, and app-only deployment
- `docker-compose.coolify.yml` — production-oriented deployment scaffold
- `scripts/` — operational and regression tooling

## Repository framing

This repository is not a generic NFT viewer and not a framework starter. Its value is in the engineering tradeoffs it exposes: how to move blockchain and media work out of request handlers, how to model read state for multiple NFT standards, how to protect internal APIs, and how to make the resulting system inspectable and operable.

## License

This repository is licensed under the MIT License. See the `LICENSE` file for details.
