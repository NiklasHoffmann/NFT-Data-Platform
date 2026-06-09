# syntax=docker/dockerfile:1.7
FROM node:20-bookworm-slim AS builder

WORKDIR /app

ENV NEXT_TELEMETRY_DISABLED=1

COPY . .

# Keep install in development mode even if Coolify passes NODE_ENV=production as build arg.
RUN --mount=type=cache,target=/root/.npm NODE_ENV=development NODE_OPTIONS=--max-old-space-size=768 npm ci --include=dev --workspace @nft-platform/web --include-workspace-root --no-audit --no-fund --loglevel=info --prefer-offline --fetch-retries=5 --fetch-retry-factor=2 --fetch-retry-mintimeout=20000 --fetch-retry-maxtimeout=120000 --fetch-timeout=180000

ENV NODE_ENV=production
RUN NODE_OPTIONS=--max-old-space-size=1536 npm run build --workspace @nft-platform/web

# ---- runner ----
FROM node:20-bookworm-slim AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

# Standalone output bundles only the required node_modules.
# Static assets and public folder must be copied separately.
COPY --from=builder /app/apps/web/.next/standalone ./
COPY --from=builder /app/apps/web/.next/static ./apps/web/.next/static

EXPOSE 3000

CMD ["node", "apps/web/server.js"]
