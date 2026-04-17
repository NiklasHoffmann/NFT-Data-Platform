import { getMongoDatabase } from "@nft-platform/db";
import { getWebRuntimeConfig } from "./env";

export function getWebMongoDatabase() {
  const config = getWebRuntimeConfig();

  return getMongoDatabase({
    uri: config.mongodbUri,
    databaseName: config.mongodbDatabase,
    appName: "nft-platform-web"
  });
}