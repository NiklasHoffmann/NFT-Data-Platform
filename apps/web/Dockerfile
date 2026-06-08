FROM node:20-bookworm-slim AS builder

WORKDIR /app

ENV NEXT_TELEMETRY_DISABLED=1

COPY . .

RUN NODE_OPTIONS=--max-old-space-size=1024 npm ci --include=dev

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
COPY --from=builder /app/apps/web/public ./apps/web/public

EXPOSE 3000

CMD ["node", "apps/web/server.js"]
