#!/bin/sh
set -eu

export MONGODB_DATABASE="${MONGODB_DATABASE:-nft_data_platform}"
export MONGODB_APP_USERNAME="${MONGODB_APP_USERNAME:-}"
export MONGODB_APP_PASSWORD="${MONGODB_APP_PASSWORD:-}"

if [ -z "$MONGODB_APP_USERNAME" ] || [ -z "$MONGODB_APP_PASSWORD" ]; then
  echo "MONGODB_APP_USERNAME and MONGODB_APP_PASSWORD must be set for MongoDB initialization." >&2
  exit 1
fi

mongosh \
  --quiet \
  --username "$MONGO_INITDB_ROOT_USERNAME" \
  --password "$MONGO_INITDB_ROOT_PASSWORD" \
  --authenticationDatabase admin <<'EOF'
const databaseName = process.env.MONGODB_DATABASE;
const appUsername = process.env.MONGODB_APP_USERNAME;
const appPassword = process.env.MONGODB_APP_PASSWORD;

const applicationDatabase = db.getSiblingDB(databaseName);
const existingUser = applicationDatabase.getUser(appUsername);

if (!existingUser) {
  applicationDatabase.createUser({
    user: appUsername,
    pwd: appPassword,
    roles: [{ role: "readWrite", db: databaseName }]
  });
}
EOF