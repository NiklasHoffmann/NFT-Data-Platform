FROM node:20-bookworm-slim

WORKDIR /app

COPY . .

# Keep install in development mode even if Coolify passes NODE_ENV=production as build arg.
RUN NODE_ENV=development NODE_OPTIONS=--max-old-space-size=768 npm ci --include=dev --workspace @nft-platform/worker --include-workspace-root --no-audit --no-fund --loglevel=info

ENV NODE_ENV=production

CMD ["npm", "run", "start", "--workspace", "@nft-platform/worker"]
