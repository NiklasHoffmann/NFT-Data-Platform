FROM node:20-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production

COPY . .

RUN npm ci

CMD ["npm", "run", "start", "--workspace", "@nft-platform/worker"]
