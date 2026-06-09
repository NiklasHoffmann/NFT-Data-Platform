# syntax=docker/dockerfile:1.7
FROM node:20-bookworm-slim

WORKDIR /app

COPY . .

# Keep install in development mode even if Coolify passes NODE_ENV=production as build arg.
RUN --mount=type=cache,target=/root/.npm NODE_ENV=development NODE_OPTIONS=--max-old-space-size=768 npm ci --include=dev --workspace @nft-platform/worker --include-workspace-root --no-audit --no-fund --loglevel=info --prefer-offline --fetch-retries=5 --fetch-retry-factor=2 --fetch-retry-mintimeout=20000 --fetch-retry-maxtimeout=120000 --fetch-timeout=180000

ENV NODE_ENV=production

CMD ["npm", "run", "start", "--workspace", "@nft-platform/worker"]
