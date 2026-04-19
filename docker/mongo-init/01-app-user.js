const databaseName = process.env.MONGODB_DATABASE || "nft_data_platform";
const appUsername = process.env.MONGODB_APP_USERNAME;
const appPassword = process.env.MONGODB_APP_PASSWORD;

if (!appUsername || !appPassword) {
  throw new Error("MONGODB_APP_USERNAME and MONGODB_APP_PASSWORD must be set for MongoDB initialization.");
}

const applicationDatabase = db.getSiblingDB(databaseName);
const existingUser = applicationDatabase.getUser(appUsername);

if (!existingUser) {
  applicationDatabase.createUser({
    user: appUsername,
    pwd: appPassword,
    roles: [{ role: "readWrite", db: databaseName }]
  });
}