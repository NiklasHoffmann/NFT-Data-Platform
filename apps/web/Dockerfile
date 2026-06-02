FROM node:20-bookworm-slim

WORKDIR /app

ENV NEXT_TELEMETRY_DISABLED=1

COPY . .

RUN NODE_OPTIONS=--max-old-space-size=1024 npm ci --include=dev

ENV NODE_ENV=production
RUN NODE_OPTIONS=--max-old-space-size=1536 npm run build --workspace @nft-platform/web

EXPOSE 3000

CMD ["npm", "run", "start", "--workspace", "@nft-platform/web"]
