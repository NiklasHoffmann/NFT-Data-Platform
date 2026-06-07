import process from "node:process";
import { closeMongoClientSingleton, ensureCoreIndexes, getMongoDatabase } from "@nft-platform/db";
import { loadLocalEnvFiles } from "@nft-platform/runtime";

loadLocalEnvFiles();

async function main(): Promise<void> {
  const mongodbUri = process.env.MONGODB_URI ?? "mongodb://localhost:27017";
  const mongodbDatabase = process.env.MONGODB_DATABASE ?? "nft_data_platform";

  const database = getMongoDatabase({
    uri: mongodbUri,
    databaseName: mongodbDatabase,
    appName: "nft-platform-ensure-indexes"
  });

  await ensureCoreIndexes(database);
  console.log("[db:ensure-indexes] All indexes ensured.");

  await closeMongoClientSingleton({
    uri: mongodbUri,
    appName: "nft-platform-ensure-indexes"
  });
}

main().catch((error) => {
  console.error("[db:ensure-indexes] failed", error);
  process.exit(1);
});
