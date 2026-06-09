# Coolify Step-by-Step Deployment (App-Only + External Services)

This guide deploys the application (`web` + `worker`) separately from MongoDB, Redis, and MinIO so app deploys are fast and do not restart data services.

## 1. Target Architecture

Create 4 separate Coolify applications in the same project/network:

1. `nft-data-mongo`
2. `nft-data-redis`
3. `nft-data-minio`
4. `nft-data-platform` (this repository with `docker-compose.coolify.yml`)

## 2. MongoDB Service

Use a dedicated Docker image app.

1. General
2. Name: `nft-data-mongo`
3. Docker Image: `mongo:8.0`
4. Command: leave default
5. Domains: none
6. Ports Exposes: `27017`
7. Port Mappings: none
8. Custom Docker Options: empty
9. Pre/Post Deployment Commands: empty
10. Storage
11. Add persistent volume mounted to `/data/db`
12. Environment
13. `MONGO_INITDB_ROOT_USERNAME=root`
14. `MONGO_INITDB_ROOT_PASSWORD=<strong-password>`
15. Deploy and wait for healthy status.

Notes:

1. If Coolify cannot change the initial database field later, that is fine.
2. The application uses `MONGODB_DATABASE=nft_data_platform` and creates collections on first bootstrap.

## 3. Redis Service

Use a dedicated Docker image app.

1. General
2. Name: `nft-data-redis`
3. Docker Image: `redis:7.4-alpine`
4. Command: `redis-server --appendonly yes --requirepass <strong-password>`
5. Domains: none
6. Ports Exposes: `6379`
7. Port Mappings: none
8. Custom Docker Options: empty
9. Pre/Post Deployment Commands: empty
10. Storage
11. Add persistent volume mounted to `/data`
12. Deploy and wait for healthy status.

## 4. MinIO Service

Use a dedicated Docker image app.

1. General
2. Name: `nft-data-minio`
3. Docker Image: `minio/minio:latest`
4. Command: `minio server /data --console-address :9001`
5. Domains
6. S3 API domain -> port `9000` (required)
7. Console domain -> port `9001` (optional)
8. Ports Exposes: `9000` and `9001`
9. Port Mappings: none
10. Custom Docker Options: empty
11. Pre/Post Deployment Commands: empty
12. Environment
13. `MINIO_ROOT_USER=<admin-user>`
14. `MINIO_ROOT_PASSWORD=<strong-password>`
15. Storage
16. Add persistent volume mounted to `/data`
17. Deploy and wait for healthy status.

After deploy:

1. Open MinIO console.
2. Create bucket: `nft-media`.
3. Create a dedicated access key/secret for the app (do not use root for application runtime).

## 5. App Deployment From This Repository

Deploy this repository as a separate Coolify application using `docker-compose.coolify.yml`.

1. General
2. Name: `nft-data-platform`
3. Source: this git repository
4. Compose file: `docker-compose.coolify.yml`
5. Domain: attach to service `web` and route to port `3000`

Set environment variables from `.env.coolify.example`.

Required core variables:

1. `NODE_ENV=production`
2. `APP_BASE_URL=https://<your-app-domain>`
3. `MONGODB_URI=mongodb://root:<url-encoded-password>@nft-data-mongo:27017/?authSource=admin`
4. `MONGODB_DATABASE=nft_data_platform`
5. `REDIS_URL=redis://:<url-encoded-password>@nft-data-redis:6379`
6. `S3_ENDPOINT=http://nft-data-minio:9000`
7. `S3_REGION=us-east-1`
8. `S3_ACCESS_KEY=<minio-app-access-key>`
9. `S3_SECRET_KEY=<minio-app-secret-key>`
10. `S3_BUCKET=nft-media`
11. `S3_PUBLIC_BASE_URL=https://<your-minio-s3-domain>/nft-media`

RPC variables:

1. Set `RPC_URL_<chainId>` entries you need (for example `RPC_URL_1`, `RPC_URL_11155111`).

Auth/bootstrap variables:

1. `API_CLIENT_SECRET_ENCRYPTION_KEY=<64-hex-or-base64-32-byte-key>`
2. `API_BOOTSTRAP_CLIENT_ID=<id>`
3. `API_BOOTSTRAP_KEY=<key>`
4. `API_BOOTSTRAP_SECRET=<secret>`
5. `API_BOOTSTRAP_SCOPES=<comma-separated-scopes>`
6. `API_BOOTSTRAP_RATE_LIMIT_PER_MINUTE=300`
7. `API_BOOTSTRAP_ALLOWED_IPS=`
8. `AUTH_MAX_TIMESTAMP_SKEW_SEC=300`

Optional web controls:

1. `CORS_ALLOWED_ORIGINS=https://<your-app-domain>`
2. `API_MAX_REQUEST_BYTES=1048576`
3. `PUBLIC_READ_RATE_LIMIT_PER_MINUTE=180`

## 6. One-Time Database Bootstrap

In production mode, the worker does not auto-bootstrap validators and indexes. Run this once after first deploy.

PowerShell example from a trusted machine with repo checkout:

```powershell
$env:MONGODB_URI = "mongodb://root:<url-encoded-password>@<mongo-host>:27017/?authSource=admin"
$env:MONGODB_DATABASE = "nft_data_platform"
$env:API_CLIENT_SECRET_ENCRYPTION_KEY = "<same-value-as-coolify>"
$env:API_BOOTSTRAP_CLIENT_ID = "<same-value-as-coolify>"
$env:API_BOOTSTRAP_KEY = "<same-value-as-coolify>"
$env:API_BOOTSTRAP_SECRET = "<same-value-as-coolify>"
$env:API_BOOTSTRAP_SCOPES = "collections:read,tokens:read,owners:read,search:read,refresh:token,refresh:collection,refresh:media,reindex:write,admin:read"
npm run db:init
```

## 7. Validation Checklist

1. `GET /api/health/live` returns `200` quickly.
2. `GET /api/health` returns all dependencies as `ok`.
3. Trigger one media refresh and verify storage object appears in `nft-media`.
4. Redeploy app and confirm Mongo/Redis/MinIO containers are not restarted.

## 8. Common Misconfigurations

1. MinIO app configured with port `80` or mapping `3000:3000`.
2. Leftover pre-deploy command like `php artisan migrate`.
3. Missing persistent volume on Mongo/Redis/MinIO.
4. Passwords in URIs not URL-encoded.
5. `S3_PUBLIC_BASE_URL` not matching your MinIO S3 domain + bucket path.
