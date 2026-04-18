FROM node:20-bookworm-slim

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1

COPY . .

RUN npm ci
RUN npm run build --workspace @nft-platform/web

EXPOSE 3000

CMD ["npm", "run", "start", "--workspace", "@nft-platform/web"]
