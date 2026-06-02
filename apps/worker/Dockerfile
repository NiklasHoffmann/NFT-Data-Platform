FROM node:20-bookworm-slim

WORKDIR /app

COPY . .

RUN NODE_OPTIONS=--max-old-space-size=1024 npm ci --include=dev

ENV NODE_ENV=production

CMD ["npm", "run", "start", "--workspace", "@nft-platform/worker"]
