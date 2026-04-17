import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  transpilePackages: [
    "@nft-platform/chain",
    "@nft-platform/db",
    "@nft-platform/domain",
    "@nft-platform/queue",
    "@nft-platform/security",
    "@nft-platform/storage"
  ],
  images: {
    localPatterns: [
      {
        pathname: "/api/media"
      }
    ]
  }
};

export default nextConfig;
