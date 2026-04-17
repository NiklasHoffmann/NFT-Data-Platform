import {
  MongoClient,
  ObjectId,
  type Collection,
  type Db,
  type Filter,
  type MongoClientOptions,
  type OptionalUnlessRequiredId
} from "mongodb";
import {
  apiClientStatusSchema,
  auditRateLimitDecisionSchema,
  interactiveMediaTypeSchema,
  jobStatusSchema,
  jobTypeSchema,
  mediaKindSchema,
  mediaStatusSchema,
  metadataStatusSchema,
  nftStandardSchema,
  normalizeContractAddress,
  normalizeWalletAddress,
  scopeSchema,
  syncStatusSchema,
  type ApiClientStatus,
  type AuditRateLimitDecision,
  type InteractiveMediaType,
  type JobStatus,
  type JobType,
  type MediaKind,
  type MediaStatus,
  type MetadataStatus,
  type NftStandard,
  type Scope,
  type SyncStatus,
  type TokenAttribute
} from "@nft-platform/domain";
import { buildApiKeyPrefix, encryptSecret, sha256Hex } from "@nft-platform/security";

export const mongoCollectionNames = {
  collections: "collections",
  tokens: "tokens",
  erc721Ownership: "erc721_ownership",
  erc1155Balances: "erc1155_balances",
  metadataVersions: "metadata_versions",
  mediaAssets: "media_assets",
  jobs: "jobs",
  apiClients: "api_clients",
  auditLogs: "audit_logs"
} as const;

export type MongoCollectionName =
  (typeof mongoCollectionNames)[keyof typeof mongoCollectionNames];

export type CollectionDocument = {
  _id: ObjectId;
  chainId: number;
  contractAddress: string;
  standard: NftStandard;
  name: string | null;
  symbol: string | null;
  baseUri: string | null;
  contractUriRaw: string | null;
  contractUriResolved: string | null;
  creatorName: string | null;
  creatorAddress: string | null;
  contractOwnerAddress: string | null;
  royaltyRecipientAddress: string | null;
  royaltyBasisPoints: number | null;
  collectionMetadataPayload: Record<string, unknown> | null;
  collectionMetadataHash: string | null;
  lastCollectionMetadataFetchAt: Date | null;
  lastCollectionMetadataError: string | null;
  description: string | null;
  externalUrl: string | null;
  imageOriginalUrl: string | null;
  bannerImageOriginalUrl: string | null;
  featuredImageOriginalUrl: string | null;
  animationOriginalUrl: string | null;
  audioOriginalUrl: string | null;
  interactiveOriginalUrl: string | null;
  totalSupply: string | null;
  indexedTokenCount: number;
  deployBlock: number | null;
  lastObservedBlock: number | null;
  lastIndexedBlock: number | null;
  syncStatus: SyncStatus;
  lastSyncAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

export type TokenDocument = {
  _id: ObjectId;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  standard: NftStandard;
  metadataUriRaw: string | null;
  metadataUriResolved: string | null;
  supplyQuantity: string | null;
  metadataStatus: MetadataStatus;
  metadataVersion: number;
  metadataHash: string | null;
  metadataPayload: Record<string, unknown> | null;
  lastMetadataError: string | null;
  name: string | null;
  description: string | null;
  externalUrl: string | null;
  imageOriginalUrl: string | null;
  imageAssetId: ObjectId | null;
  animationOriginalUrl: string | null;
  animationAssetId: ObjectId | null;
  audioOriginalUrl: string | null;
  audioAssetId: ObjectId | null;
  interactiveOriginalUrl: string | null;
  interactiveMediaType: InteractiveMediaType | null;
  attributes: TokenAttribute[];
  mediaStatus: MediaStatus;
  ownerStateVersion: number;
  lastMetadataFetchAt: Date | null;
  lastMediaProcessAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

export type Erc721OwnershipDocument = {
  _id: ObjectId;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  ownerAddress: string;
  updatedAt: Date;
};

export type Erc1155BalanceDocument = {
  _id: ObjectId;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  ownerAddress: string;
  balance: string;
  updatedAt: Date;
};

export type MetadataVersionDocument = {
  _id: ObjectId;
  tokenRef: ObjectId;
  version: number;
  sourceUri: string;
  payload: Record<string, unknown>;
  payloadHash: string;
  fetchedAt: Date;
};

export type MediaAssetDocument = {
  _id: ObjectId;
  tokenRef: ObjectId;
  kind: MediaKind;
  sourceUrl: string;
  storageKeyOriginal: string;
  storageKeyOptimized: string | null;
  storageKeyThumbnail: string | null;
  cdnUrlOriginal: string | null;
  cdnUrlOptimized: string | null;
  cdnUrlThumbnail: string | null;
  mimeType: string | null;
  sizeBytes: number | null;
  checksumSha256: string | null;
  width: number | null;
  height: number | null;
  durationSec: number | null;
  status: MediaStatus;
  statusDetail: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type JobDocument = {
  _id: ObjectId;
  queueJobId: string | null;
  type: JobType;
  payload: Record<string, unknown>;
  status: JobStatus;
  attempts: number;
  lastError: string | null;
  createdAt: Date;
  updatedAt: Date;
};

export type ApiClientDocument = {
  _id: ObjectId;
  clientId: string;
  clientName: string;
  keyPrefix: string;
  keyHash: string;
  secretEncrypted: string | null;
  scopes: Scope[];
  rateLimitPerMinute: number;
  allowedIps: string[];
  status: ApiClientStatus;
  lastUsedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

export type AuditLogDocument = {
  _id: ObjectId;
  clientId: string;
  scope: Scope | null;
  method: string;
  path: string;
  statusCode: number;
  responseTimeMs: number;
  ip: string | null;
  timestamp: Date;
  rateLimitDecision: AuditRateLimitDecision;
};

export type MongoCollections = {
  collections: Collection<CollectionDocument>;
  tokens: Collection<TokenDocument>;
  erc721Ownership: Collection<Erc721OwnershipDocument>;
  erc1155Balances: Collection<Erc1155BalanceDocument>;
  metadataVersions: Collection<MetadataVersionDocument>;
  mediaAssets: Collection<MediaAssetDocument>;
  jobs: Collection<JobDocument>;
  apiClients: Collection<ApiClientDocument>;
  auditLogs: Collection<AuditLogDocument>;
};

export type BootstrapApiClientSeed = {
  clientId: string;
  clientName: string;
  apiKey: string;
  apiSecret: string;
  scopes: Scope[];
  rateLimitPerMinute: number;
  allowedIps: string[];
  encryptionKey: string;
};

type IndexModel = {
  key: Record<string, 1 | -1>;
  name: string;
  unique?: boolean;
  sparse?: boolean;
};

type MongoSchema = Record<string, unknown>;

type CollectionDefinition = {
  validator: {
    $jsonSchema: MongoSchema;
  };
  validationAction: "error";
  validationLevel: "moderate";
};

const integerLikeBsonTypes = ["int", "long", "double", "decimal"];
const httpMethods = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"] as const;

const supportedRemoteUriPattern = "^(https?://|ipfs://|ar://)";
const supportedRemoteOrDataUriPattern = "^(https?://|ipfs://|ar://|data:)";
const httpUrlPattern = "^https?://";
const nonNegativeIntegerStringPattern = "^(0|[1-9][0-9]*)$";
const sha256HexPattern = "^[a-fA-F0-9]{64}$";
const queueJobIdPattern = "^[a-z0-9-]+-[a-fA-F0-9]{64}$";

function nonEmptyStringField(): MongoSchema {
  return {
    bsonType: "string",
    minLength: 1
  };
}

function nullableField(schema: MongoSchema): MongoSchema {
  return {
    anyOf: [schema, { bsonType: "null" }]
  };
}

function enumStringField(values: readonly string[]): MongoSchema {
  return {
    bsonType: "string",
    enum: [...values]
  };
}

function patternedStringField(pattern: string, options?: { minLength?: number; maxLength?: number }): MongoSchema {
  const schema: MongoSchema = {
    bsonType: "string",
    pattern
  };

  if (options?.minLength !== undefined) {
    schema.minLength = options.minLength;
  }

  if (options?.maxLength !== undefined) {
    schema.maxLength = options.maxLength;
  }

  return schema;
}

const addressField: MongoSchema = {
  bsonType: "string",
  pattern: "^0x[a-f0-9]{40}$"
};

const objectIdField: MongoSchema = {
  bsonType: "objectId"
};

const dateField: MongoSchema = {
  bsonType: "date"
};

const nonNegativeNumberField: MongoSchema = {
  bsonType: integerLikeBsonTypes,
  minimum: 0
};

const positiveNumberField: MongoSchema = {
  bsonType: integerLikeBsonTypes,
  minimum: 1
};

const nonNegativeIntegerStringField = patternedStringField(nonNegativeIntegerStringPattern, {
  minLength: 1
});

const sha256HexField = patternedStringField(sha256HexPattern, {
  minLength: 64,
  maxLength: 64
});

const supportedRemoteUriField = patternedStringField(supportedRemoteUriPattern, {
  minLength: 1
});

const supportedRemoteOrDataUriField = patternedStringField(supportedRemoteOrDataUriPattern, {
  minLength: 1
});

const httpUrlField = patternedStringField(httpUrlPattern, {
  minLength: 1
});

const queueJobIdField = patternedStringField(queueJobIdPattern, {
  minLength: 1
});

const apiKeyPrefixField = {
  bsonType: "string",
  minLength: 1,
  maxLength: 12
} satisfies MongoSchema;

const pathField = {
  bsonType: "string",
  minLength: 1,
  pattern: "^/"
} satisfies MongoSchema;

const tokenAttributeField: MongoSchema = {
  bsonType: "object",
  properties: {
    trait_type: nonEmptyStringField(),
    value: {
      anyOf: [
        nonEmptyStringField(),
        { bsonType: integerLikeBsonTypes },
        { bsonType: "bool" }
      ]
    },
    display_type: nonEmptyStringField()
  }
};

const collectionDefinitions: Record<MongoCollectionName, CollectionDefinition> = {
  collections: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: [
          "chainId",
          "contractAddress",
          "standard",
          "syncStatus",
          "createdAt",
          "updatedAt"
        ],
        properties: {
          _id: objectIdField,
          chainId: positiveNumberField,
          contractAddress: addressField,
          standard: enumStringField(nftStandardSchema.options),
          name: nullableField(nonEmptyStringField()),
          symbol: nullableField(nonEmptyStringField()),
          baseUri: nullableField(supportedRemoteUriField),
          contractUriRaw: nullableField(supportedRemoteUriField),
          contractUriResolved: nullableField(supportedRemoteUriField),
          creatorName: nullableField(nonEmptyStringField()),
          creatorAddress: nullableField(addressField),
          contractOwnerAddress: nullableField(addressField),
          royaltyRecipientAddress: nullableField(addressField),
          royaltyBasisPoints: nullableField(nonNegativeNumberField),
          collectionMetadataPayload: nullableField({ bsonType: "object" }),
          collectionMetadataHash: nullableField(nonEmptyStringField()),
          lastCollectionMetadataFetchAt: nullableField(dateField),
          lastCollectionMetadataError: nullableField(nonEmptyStringField()),
          description: nullableField(nonEmptyStringField()),
          externalUrl: nullableField(supportedRemoteUriField),
          imageOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          bannerImageOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          featuredImageOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          animationOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          audioOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          interactiveOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          totalSupply: nullableField(nonNegativeIntegerStringField),
          indexedTokenCount: nonNegativeNumberField,
          deployBlock: nullableField(nonNegativeNumberField),
          lastObservedBlock: nullableField(nonNegativeNumberField),
          lastIndexedBlock: nullableField(nonNegativeNumberField),
          syncStatus: enumStringField(syncStatusSchema.options),
          lastSyncAt: nullableField(dateField),
          createdAt: dateField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  tokens: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: [
          "chainId",
          "contractAddress",
          "tokenId",
          "standard",
          "metadataStatus",
          "metadataVersion",
          "attributes",
          "mediaStatus",
          "ownerStateVersion",
          "createdAt",
          "updatedAt"
        ],
        properties: {
          _id: objectIdField,
          chainId: positiveNumberField,
          contractAddress: addressField,
          tokenId: nonEmptyStringField(),
          standard: enumStringField(nftStandardSchema.options),
          metadataUriRaw: nullableField(supportedRemoteOrDataUriField),
          metadataUriResolved: nullableField(supportedRemoteOrDataUriField),
          supplyQuantity: nullableField(nonNegativeIntegerStringField),
          metadataStatus: enumStringField(metadataStatusSchema.options),
          metadataVersion: nonNegativeNumberField,
          metadataHash: nullableField(sha256HexField),
          metadataPayload: nullableField({ bsonType: "object" }),
          lastMetadataError: nullableField(nonEmptyStringField()),
          name: nullableField(nonEmptyStringField()),
          description: nullableField(nonEmptyStringField()),
          externalUrl: nullableField(supportedRemoteUriField),
          imageOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          imageAssetId: nullableField(objectIdField),
          animationOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          animationAssetId: nullableField(objectIdField),
          audioOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          audioAssetId: nullableField(objectIdField),
          interactiveOriginalUrl: nullableField(supportedRemoteOrDataUriField),
          interactiveMediaType: nullableField(enumStringField(interactiveMediaTypeSchema.options)),
          attributes: {
            bsonType: "array",
            items: tokenAttributeField
          },
          mediaStatus: enumStringField(mediaStatusSchema.options),
          ownerStateVersion: nonNegativeNumberField,
          lastMetadataFetchAt: nullableField(dateField),
          lastMediaProcessAt: nullableField(dateField),
          createdAt: dateField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  erc721_ownership: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["chainId", "contractAddress", "tokenId", "ownerAddress", "updatedAt"],
        properties: {
          _id: objectIdField,
          chainId: positiveNumberField,
          contractAddress: addressField,
          tokenId: nonEmptyStringField(),
          ownerAddress: addressField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  erc1155_balances: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["chainId", "contractAddress", "tokenId", "ownerAddress", "balance", "updatedAt"],
        properties: {
          _id: objectIdField,
          chainId: positiveNumberField,
          contractAddress: addressField,
          tokenId: nonEmptyStringField(),
          ownerAddress: addressField,
          balance: nonNegativeIntegerStringField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  metadata_versions: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["tokenRef", "version", "sourceUri", "payload", "payloadHash", "fetchedAt"],
        properties: {
          _id: objectIdField,
          tokenRef: objectIdField,
          version: positiveNumberField,
          sourceUri: nonEmptyStringField(),
          payload: {
            bsonType: "object"
          },
          payloadHash: sha256HexField,
          fetchedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  media_assets: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: [
          "tokenRef",
          "kind",
          "sourceUrl",
          "storageKeyOriginal",
          "status",
          "createdAt",
          "updatedAt"
        ],
        properties: {
          _id: objectIdField,
          tokenRef: objectIdField,
          kind: enumStringField(mediaKindSchema.options),
          sourceUrl: supportedRemoteOrDataUriField,
          storageKeyOriginal: nonEmptyStringField(),
          storageKeyOptimized: nullableField(nonEmptyStringField()),
          storageKeyThumbnail: nullableField(nonEmptyStringField()),
          cdnUrlOriginal: nullableField(httpUrlField),
          cdnUrlOptimized: nullableField(httpUrlField),
          cdnUrlThumbnail: nullableField(httpUrlField),
          mimeType: nullableField(nonEmptyStringField()),
          sizeBytes: nullableField(nonNegativeNumberField),
          checksumSha256: nullableField(sha256HexField),
          width: nullableField(positiveNumberField),
          height: nullableField(positiveNumberField),
          durationSec: nullableField(nonNegativeNumberField),
          status: enumStringField(mediaStatusSchema.options),
          statusDetail: nullableField(nonEmptyStringField()),
          createdAt: dateField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  jobs: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: ["type", "payload", "status", "attempts", "createdAt", "updatedAt"],
        properties: {
          _id: objectIdField,
          queueJobId: nullableField(queueJobIdField),
          type: enumStringField(jobTypeSchema.options),
          payload: {
            bsonType: "object"
          },
          status: enumStringField(jobStatusSchema.options),
          attempts: nonNegativeNumberField,
          lastError: nullableField(nonEmptyStringField()),
          createdAt: dateField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  api_clients: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: [
          "clientId",
          "clientName",
          "keyPrefix",
          "keyHash",
          "scopes",
          "rateLimitPerMinute",
          "allowedIps",
          "status",
          "createdAt",
          "updatedAt"
        ],
        properties: {
          _id: objectIdField,
          clientId: nonEmptyStringField(),
          clientName: nonEmptyStringField(),
          keyPrefix: apiKeyPrefixField,
          keyHash: sha256HexField,
          secretEncrypted: nullableField(nonEmptyStringField()),
          scopes: {
            bsonType: "array",
            items: enumStringField(scopeSchema.options)
          },
          rateLimitPerMinute: positiveNumberField,
          allowedIps: {
            bsonType: "array",
            items: {
              bsonType: "string"
            }
          },
          status: enumStringField(apiClientStatusSchema.options),
          lastUsedAt: nullableField(dateField),
          createdAt: dateField,
          updatedAt: dateField
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  },
  audit_logs: {
    validator: {
      $jsonSchema: {
        bsonType: "object",
        required: [
          "clientId",
          "method",
          "path",
          "statusCode",
          "responseTimeMs",
          "timestamp",
          "rateLimitDecision"
        ],
        properties: {
          _id: objectIdField,
          clientId: nonEmptyStringField(),
          scope: nullableField(enumStringField(scopeSchema.options)),
          method: enumStringField(httpMethods),
          path: pathField,
          statusCode: {
            bsonType: integerLikeBsonTypes,
            minimum: 100,
            maximum: 599
          },
          responseTimeMs: nonNegativeNumberField,
          ip: nullableField(nonEmptyStringField()),
          timestamp: dateField,
          rateLimitDecision: enumStringField(auditRateLimitDecisionSchema.options)
        }
      }
    },
    validationAction: "error",
    validationLevel: "moderate"
  }
};

const collectionIndexes: Record<MongoCollectionName, IndexModel[]> = {
  collections: [
    { key: { chainId: 1, contractAddress: 1 }, name: "uniq_chain_contract", unique: true },
    { key: { syncStatus: 1, updatedAt: -1 }, name: "sync_status_updated_at" },
    { key: { syncStatus: 1, standard: 1, lastSyncAt: 1, updatedAt: 1 }, name: "auto_index_scan" }
  ],
  tokens: [
    {
      key: { chainId: 1, contractAddress: 1, tokenId: 1 },
      name: "uniq_chain_contract_token",
      unique: true
    },
    { key: { metadataStatus: 1, updatedAt: -1 }, name: "metadata_status_updated_at" },
    { key: { mediaStatus: 1, updatedAt: -1 }, name: "media_status_updated_at" },
    { key: { updatedAt: -1 }, name: "updated_at_desc" },
    { key: { "attributes.trait_type": 1, "attributes.value": 1 }, name: "trait_value_lookup" }
  ],
  erc721_ownership: [
    { key: { chainId: 1, contractAddress: 1, tokenId: 1 }, name: "uniq_token_owner", unique: true },
    { key: { ownerAddress: 1, contractAddress: 1 }, name: "owner_contract_lookup" }
  ],
  erc1155_balances: [
    {
      key: { chainId: 1, contractAddress: 1, tokenId: 1, ownerAddress: 1 },
      name: "uniq_balance_owner_token",
      unique: true
    },
    { key: { chainId: 1, contractAddress: 1, tokenId: 1, updatedAt: -1 }, name: "token_updated_at_desc" },
    { key: { ownerAddress: 1, contractAddress: 1 }, name: "owner_contract_balance_lookup" }
  ],
  metadata_versions: [
    { key: { tokenRef: 1, version: -1 }, name: "token_version_desc", unique: true },
    { key: { payloadHash: 1 }, name: "payload_hash_lookup" }
  ],
  media_assets: [
    { key: { tokenRef: 1, kind: 1 }, name: "token_kind_lookup", unique: true },
    { key: { checksumSha256: 1 }, name: "checksum_lookup", sparse: true }
  ],
  jobs: [
    { key: { queueJobId: 1 }, name: "uniq_queue_job_id", unique: true, sparse: true },
    { key: { status: 1, createdAt: -1 }, name: "status_created_at" },
    { key: { type: 1, status: 1, updatedAt: -1 }, name: "type_status_updated_at" }
  ],
  api_clients: [
    { key: { clientId: 1 }, name: "uniq_client_id", unique: true },
    { key: { keyHash: 1 }, name: "uniq_key_hash", unique: true },
    { key: { status: 1, updatedAt: -1 }, name: "status_updated_at" }
  ],
  audit_logs: [
    { key: { clientId: 1, timestamp: -1 }, name: "client_timestamp_desc" },
    { key: { timestamp: -1 }, name: "timestamp_desc" }
  ]
};

const defaultMongoClientOptions: MongoClientOptions = {
  ignoreUndefined: true,
  connectTimeoutMS: 10000,
  serverSelectionTimeoutMS: 5000,
  maxIdleTimeMS: 300000
};

const globalMongoRegistry = globalThis as typeof globalThis & {
  __nftPlatformMongoClients__?: Map<string, MongoClient>;
};

export function getMongoCollectionNames(): MongoCollectionName[] {
  return Object.values(mongoCollectionNames);
}

export function createMongoClient(params: {
  uri: string;
  appName: string;
  options?: MongoClientOptions;
}): MongoClient {
  return new MongoClient(params.uri, {
    appName: params.appName,
    ...defaultMongoClientOptions,
    ...params.options
  });
}

export function getMongoClientSingleton(params: {
  uri: string;
  appName: string;
  options?: MongoClientOptions;
}): MongoClient {
  const registry = (globalMongoRegistry.__nftPlatformMongoClients__ ??= new Map());
  const cacheKey = `${params.appName}:${params.uri}`;
  const existingClient = registry.get(cacheKey);

  if (existingClient) {
    return existingClient;
  }

  const client = createMongoClient(params);
  registry.set(cacheKey, client);
  return client;
}

export async function closeMongoClientSingleton(params: {
  uri: string;
  appName: string;
}): Promise<void> {
  const registry = globalMongoRegistry.__nftPlatformMongoClients__;

  if (!registry) {
    return;
  }

  const cacheKey = `${params.appName}:${params.uri}`;
  const client = registry.get(cacheKey);

  if (!client) {
    return;
  }

  registry.delete(cacheKey);
  await client.close();
}

export async function closeAllMongoClients(): Promise<void> {
  const registry = globalMongoRegistry.__nftPlatformMongoClients__;

  if (!registry) {
    return;
  }

  const clients = [...registry.values()];
  registry.clear();
  await Promise.all(clients.map((client) => client.close()));
}

export function getMongoDatabase(params: {
  uri: string;
  databaseName: string;
  appName: string;
  options?: MongoClientOptions;
}): Db {
  const clientParams: {
    uri: string;
    appName: string;
    options?: MongoClientOptions;
  } = {
    uri: params.uri,
    appName: params.appName
  };

  if (params.options) {
    clientParams.options = params.options;
  }

  return getMongoClientSingleton(clientParams).db(params.databaseName);
}

export function getMongoCollections(database: Db): MongoCollections {
  return {
    collections: database.collection<CollectionDocument>(mongoCollectionNames.collections),
    tokens: database.collection<TokenDocument>(mongoCollectionNames.tokens),
    erc721Ownership: database.collection<Erc721OwnershipDocument>(mongoCollectionNames.erc721Ownership),
    erc1155Balances: database.collection<Erc1155BalanceDocument>(mongoCollectionNames.erc1155Balances),
    metadataVersions: database.collection<MetadataVersionDocument>(mongoCollectionNames.metadataVersions),
    mediaAssets: database.collection<MediaAssetDocument>(mongoCollectionNames.mediaAssets),
    jobs: database.collection<JobDocument>(mongoCollectionNames.jobs),
    apiClients: database.collection<ApiClientDocument>(mongoCollectionNames.apiClients),
    auditLogs: database.collection<AuditLogDocument>(mongoCollectionNames.auditLogs)
  };
}

export async function ensureCoreCollections(database: Db): Promise<void> {
  const existingCollections = new Set(
    (await database.listCollections({}, { nameOnly: true }).toArray()).map((collection) => collection.name)
  );

  for (const [collectionName, definition] of Object.entries(collectionDefinitions) as [
    MongoCollectionName,
    CollectionDefinition
  ][]) {
    if (!existingCollections.has(collectionName)) {
      await database.createCollection(collectionName, definition);
      existingCollections.add(collectionName);
      continue;
    }

    await database.command({
      collMod: collectionName,
      validator: definition.validator,
      validationAction: definition.validationAction,
      validationLevel: definition.validationLevel
    });
  }
}

export async function ensureCoreIndexes(database: Db): Promise<void> {
  await Promise.all(
    Object.entries(collectionIndexes).map(async ([collectionName, indexes]) => {
      const collection = database.collection(collectionName);
      await collection.createIndexes(indexes);
    })
  );
}

export async function initializePlatformDatabase(params: {
  database: Db;
  bootstrapApiClient?: BootstrapApiClientSeed;
}): Promise<void> {
  await ensureCoreCollections(params.database);
  await ensureCoreIndexes(params.database);

  if (params.bootstrapApiClient) {
    await ensureBootstrapApiClient(params.database, params.bootstrapApiClient);
  }
}

export async function listCollections(params: {
  database: Db;
  limit?: number;
  chainId?: number;
  contractAddress?: string;
  queryText?: string;
  cursor?: {
    updatedAt: Date;
    id: ObjectId;
  };
}): Promise<CollectionDocument[]> {
  const query: Filter<CollectionDocument> = {};

  if (params.chainId !== undefined) {
    query.chainId = params.chainId;
  }

  if (params.contractAddress) {
    query.contractAddress = normalizeContractAddress(params.contractAddress);
  }

  const searchQuery = buildCollectionSearchQuery(params.queryText);

  if (searchQuery) {
    query.$or = searchQuery;
  }

  const cursorQuery: Filter<CollectionDocument> | null = params.cursor
    ? {
        $or: [
          {
            updatedAt: {
              $lt: params.cursor.updatedAt
            }
          },
          {
            updatedAt: params.cursor.updatedAt,
            _id: {
              $lt: params.cursor.id
            }
          }
        ]
      }
    : null;

  const finalQuery = cursorQuery
    ? Object.keys(query).length > 0
      ? ({
          $and: [query, cursorQuery]
        } satisfies Filter<CollectionDocument>)
      : cursorQuery
    : query;

  return getMongoCollections(params.database)
    .collections.find(finalQuery)
    .sort({ updatedAt: -1, _id: -1 })
    .limit(params.limit ?? 100)
    .toArray();
}

export async function listCollectionsForAutoIndexing(params: {
  database: Db;
  collectionAllowlist?: Array<{
    chainId: number;
    contractAddress: string;
  }>;
  limit?: number;
}): Promise<CollectionDocument[]> {
  const query: Filter<CollectionDocument> = {
    syncStatus: "active",
    deployBlock: {
      $ne: null
    }
  };

  if (params.collectionAllowlist && params.collectionAllowlist.length > 0) {
    query.$or = params.collectionAllowlist.map((identity) => ({
      chainId: identity.chainId,
      contractAddress: normalizeContractAddress(identity.contractAddress)
    }));
  }

  return getMongoCollections(params.database)
    .collections.find(query)
    .sort({ lastSyncAt: 1, updatedAt: 1, _id: 1 })
    .limit(params.limit ?? 25)
    .toArray();
}

function buildCollectionSearchQuery(queryText: string | undefined): Filter<CollectionDocument>["$or"] | null {
  if (!queryText?.trim()) {
    return null;
  }

  const normalized = queryText.trim();
  const escaped = escapeRegExp(normalized);
  const containsPattern = new RegExp(escaped, "i");
  const normalizedAddress = /^0x[a-fA-F0-9]{40}$/.test(normalized)
    ? normalizeContractAddress(normalized)
    : null;
  const clauses: Filter<CollectionDocument>[] = [
    { name: containsPattern },
    { symbol: containsPattern },
    { contractAddress: containsPattern }
  ];

  if (normalizedAddress) {
    clauses.push({ contractAddress: normalizedAddress });
  }

  return clauses;
}

export async function findCollectionByIdentity(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
}): Promise<CollectionDocument | null> {
  return getMongoCollections(params.database).collections.findOne({
    chainId: params.chainId,
    contractAddress: normalizeContractAddress(params.contractAddress)
  });
}

export async function findLatestTokenForCollection(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
}): Promise<TokenDocument | null> {
  return getMongoCollections(params.database).tokens.findOne(
    {
      chainId: params.chainId,
      contractAddress: normalizeContractAddress(params.contractAddress)
    },
    {
      sort: { updatedAt: -1 }
    }
  );
}

export async function findLatestTokensForCollections(params: {
  database: Db;
  collections: Array<{ chainId: number; contractAddress: string }>;
}): Promise<TokenDocument[]> {
  if (params.collections.length === 0) {
    return [];
  }

  const identities = params.collections.map((collection) => ({
    chainId: collection.chainId,
    contractAddress: normalizeContractAddress(collection.contractAddress)
  }));

  return getMongoCollections(params.database).tokens.aggregate<TokenDocument>([
    {
      $match: {
        $or: identities
      }
    },
    {
      $addFields: {
        _previewRank: {
          $switch: {
            branches: [
              {
                case: {
                  $or: [
                    { $ne: ["$imageAssetId", null] },
                    { $ne: ["$imageOriginalUrl", null] }
                  ]
                },
                then: 0
              },
              {
                case: {
                  $or: [
                    { $ne: ["$animationAssetId", null] },
                    { $ne: ["$animationOriginalUrl", null] }
                  ]
                },
                then: 1
              },
              {
                case: {
                  $or: [
                    { $ne: ["$audioAssetId", null] },
                    { $ne: ["$audioOriginalUrl", null] }
                  ]
                },
                then: 2
              }
            ],
            default: 3
          }
        },
        _mediaStateRank: {
          $switch: {
            branches: [
              {
                case: { $in: ["$mediaStatus", ["ready", "partial"]] },
                then: 0
              },
              {
                case: { $eq: ["$mediaStatus", "processing"] },
                then: 1
              },
              {
                case: { $eq: ["$mediaStatus", "pending"] },
                then: 2
              }
            ],
            default: 3
          }
        }
      }
    },
    {
      $sort: {
        _previewRank: 1,
        _mediaStateRank: 1,
        updatedAt: -1
      }
    },
    {
      $group: {
        _id: {
          chainId: "$chainId",
          contractAddress: "$contractAddress"
        },
        document: { $first: "$$ROOT" }
      }
    },
    {
      $replaceRoot: {
        newRoot: "$document"
      }
    }
  ]).toArray();
}

export async function countTokensForCollections(params: {
  database: Db;
  collections: Array<{ chainId: number; contractAddress: string }>;
}): Promise<Map<string, number>> {
  if (params.collections.length === 0) {
    return new Map();
  }

  const identities = params.collections.map((collection) => ({
    chainId: collection.chainId,
    contractAddress: normalizeContractAddress(collection.contractAddress)
  }));

  const counts = await getMongoCollections(params.database).tokens.aggregate<{
    _id: { chainId: number; contractAddress: string };
    count: number;
  }>([
    {
      $match: {
        $or: identities
      }
    },
    {
      $group: {
        _id: {
          chainId: "$chainId",
          contractAddress: "$contractAddress"
        },
        count: { $sum: 1 }
      }
    }
  ]).toArray();

  return new Map(
    counts.map((entry) => [`${entry._id.chainId}:${normalizeContractAddress(entry._id.contractAddress)}`, entry.count])
  );
}

export async function countHoldersForCollections(params: {
  database: Db;
  collections: Array<{ chainId: number; contractAddress: string }>;
}): Promise<Map<string, number>> {
  if (params.collections.length === 0) {
    return new Map();
  }

  const identities = params.collections.map((collection) => ({
    chainId: collection.chainId,
    contractAddress: normalizeContractAddress(collection.contractAddress)
  }));

  const [erc721Owners, erc1155Owners] = await Promise.all([
    getMongoCollections(params.database).erc721Ownership.aggregate<{
      _id: { chainId: number; contractAddress: string; ownerAddress: string };
    }>([
      {
        $match: {
          $or: identities
        }
      },
      {
        $group: {
          _id: {
            chainId: "$chainId",
            contractAddress: "$contractAddress",
            ownerAddress: "$ownerAddress"
          }
        }
      }
    ]).toArray(),
    getMongoCollections(params.database).erc1155Balances.aggregate<{
      _id: { chainId: number; contractAddress: string; ownerAddress: string };
    }>([
      {
        $match: {
          $or: identities
        }
      },
      {
        $group: {
          _id: {
            chainId: "$chainId",
            contractAddress: "$contractAddress",
            ownerAddress: "$ownerAddress"
          }
        }
      }
    ]).toArray()
  ]);

  const holdersByCollection = new Map<string, Set<string>>();

  for (const entry of [...erc721Owners, ...erc1155Owners]) {
    const key = `${entry._id.chainId}:${normalizeContractAddress(entry._id.contractAddress)}`;
    const holderSet = holdersByCollection.get(key) ?? new Set<string>();
    holderSet.add(normalizeWalletAddress(entry._id.ownerAddress));
    holdersByCollection.set(key, holderSet);
  }

  return new Map(Array.from(holdersByCollection.entries()).map(([key, holders]) => [key, holders.size]));
}

export async function findRecentTokensForCollections(params: {
  database: Db;
  collections: Array<{ chainId: number; contractAddress: string }>;
  limitPerCollection?: number;
}): Promise<TokenDocument[]> {
  if (params.collections.length === 0) {
    return [];
  }

  const limitPerCollection = params.limitPerCollection ?? 6;
  const identities = params.collections.map((collection) => ({
    chainId: collection.chainId,
    contractAddress: normalizeContractAddress(collection.contractAddress)
  }));

  return getMongoCollections(params.database).tokens.aggregate<TokenDocument>([
    {
      $match: {
        $or: identities
      }
    },
    {
      $sort: {
        updatedAt: -1,
        _id: -1
      }
    },
    {
      $group: {
        _id: {
          chainId: "$chainId",
          contractAddress: "$contractAddress"
        },
        documents: { $push: "$$ROOT" }
      }
    },
    {
      $project: {
        documents: { $slice: ["$documents", limitPerCollection] }
      }
    },
    {
      $unwind: "$documents"
    },
    {
      $replaceRoot: {
        newRoot: "$documents"
      }
    }
  ]).toArray();
}

export async function findTokensByIdentities(params: {
  database: Db;
  identities: Array<{ chainId: number; contractAddress: string; tokenId: string }>;
}): Promise<TokenDocument[]> {
  if (params.identities.length === 0) {
    return [];
  }

  const identities = params.identities.map((identity) => ({
    chainId: identity.chainId,
    contractAddress: normalizeContractAddress(identity.contractAddress),
    tokenId: identity.tokenId
  }));

  return getMongoCollections(params.database).tokens.find({
    $or: identities
  }).toArray();
}

export async function upsertCollection(
  database: Db,
  document: Omit<CollectionDocument, "_id">
): Promise<void> {
  const { createdAt, ...updatableFields } = document;

  await getMongoCollections(database).collections.updateOne(
    {
      chainId: document.chainId,
      contractAddress: normalizeContractAddress(document.contractAddress)
    },
    {
      $set: {
        ...updatableFields,
        contractAddress: normalizeContractAddress(document.contractAddress),
        updatedAt: document.updatedAt
      },
      $setOnInsert: {
        createdAt
      }
    },
    { upsert: true }
  );
}

export async function findTokenByIdentity(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
  tokenId: string;
}): Promise<TokenDocument | null> {
  return getMongoCollections(params.database).tokens.findOne({
    chainId: params.chainId,
    contractAddress: normalizeContractAddress(params.contractAddress),
    tokenId: params.tokenId
  });
}

export async function listTokens(params: {
  database: Db;
  limit?: number;
  chainId?: number;
  contractAddress?: string;
  metadataStatus?: MetadataStatus;
  mediaStatus?: MediaStatus;
  traitType?: string;
  traitValue?: string | number | boolean;
  queryText?: string;
  cursor?: {
    updatedAt: Date;
    id: ObjectId;
  };
}): Promise<TokenDocument[]> {
  const query = buildTokenFilterQuery(params);

  const cursorQuery: Filter<TokenDocument> | null = params.cursor
    ? {
        $or: [
          {
            updatedAt: {
              $lt: params.cursor.updatedAt
            }
          },
          {
            updatedAt: params.cursor.updatedAt,
            _id: {
              $lt: params.cursor.id
            }
          }
        ]
      }
    : null;

  const finalQuery = cursorQuery
    ? Object.keys(query).length > 0
      ? ({
          $and: [query, cursorQuery]
        } satisfies Filter<TokenDocument>)
      : cursorQuery
    : query;

  return getMongoCollections(params.database)
    .tokens.find(finalQuery)
    .sort({ updatedAt: -1, _id: -1 })
    .limit(params.limit ?? 100)
    .toArray();
}

export async function listErc1155Balances(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  ownerAddress?: string;
  limit?: number;
  cursor?: {
    updatedAt: Date;
    id: ObjectId;
  };
}): Promise<Erc1155BalanceDocument[]> {
  const query: Filter<Erc1155BalanceDocument> = {
    chainId: params.chainId,
    contractAddress: normalizeContractAddress(params.contractAddress),
    tokenId: params.tokenId,
    balance: { $ne: "0" }
  };

  if (params.ownerAddress) {
    query.ownerAddress = normalizeWalletAddress(params.ownerAddress);
  }

  const cursorQuery: Filter<Erc1155BalanceDocument> | null = params.cursor
    ? {
        $or: [
          {
            updatedAt: {
              $lt: params.cursor.updatedAt
            }
          },
          {
            updatedAt: params.cursor.updatedAt,
            _id: {
              $lt: params.cursor.id
            }
          }
        ]
      }
    : null;

  const finalQuery = cursorQuery
    ? ({
        $and: [query, cursorQuery]
      } satisfies Filter<Erc1155BalanceDocument>)
    : query;

  return getMongoCollections(params.database)
    .erc1155Balances.find(finalQuery)
    .sort({ updatedAt: -1, _id: -1 })
    .limit(params.limit ?? 100)
    .toArray();
}

export async function listErc1155BalancesByOwner(params: {
  database: Db;
  chainId: number;
  ownerAddress: string;
  contractAddress?: string;
  metadataStatus?: MetadataStatus;
  mediaStatus?: MediaStatus;
  traitType?: string;
  traitValue?: string | number | boolean;
  queryText?: string;
  limit?: number;
  cursor?: {
    updatedAt: Date;
    id: ObjectId;
  };
}): Promise<Erc1155BalanceDocument[]> {
  const query: Filter<Erc1155BalanceDocument> = {
    chainId: params.chainId,
    ownerAddress: normalizeWalletAddress(params.ownerAddress),
    balance: { $ne: "0" }
  };

  if (params.contractAddress) {
    query.contractAddress = normalizeContractAddress(params.contractAddress);
  }

  const tokenFilterParams: {
    metadataStatus?: MetadataStatus;
    mediaStatus?: MediaStatus;
    traitType?: string;
    traitValue?: string | number | boolean;
    queryText?: string;
  } = {};

  if (params.metadataStatus) {
    tokenFilterParams.metadataStatus = params.metadataStatus;
  }

  if (params.mediaStatus) {
    tokenFilterParams.mediaStatus = params.mediaStatus;
  }

  if (params.traitType) {
    tokenFilterParams.traitType = params.traitType;
  }

  if (params.traitValue !== undefined) {
    tokenFilterParams.traitValue = params.traitValue;
  }

  if (params.queryText) {
    tokenFilterParams.queryText = params.queryText;
  }

  const tokenQuery = buildTokenFilterQuery(tokenFilterParams);

  const cursorQuery: Filter<Erc1155BalanceDocument> | null = params.cursor
    ? {
        $or: [
          {
            updatedAt: {
              $lt: params.cursor.updatedAt
            }
          },
          {
            updatedAt: params.cursor.updatedAt,
            _id: {
              $lt: params.cursor.id
            }
          }
        ]
      }
    : null;

  const finalQuery = cursorQuery
    ? ({
        $and: [query, cursorQuery]
      } satisfies Filter<Erc1155BalanceDocument>)
    : query;

  const lookupPipeline: Array<Record<string, unknown>> = [
    {
      $match: {
        $expr: {
          $and: [
            { $eq: ["$chainId", "$$chainId"] },
            { $eq: ["$contractAddress", "$$contractAddress"] },
            { $eq: ["$tokenId", "$$tokenId"] }
          ]
        }
      }
    }
  ];

  if (Object.keys(tokenQuery).length > 0) {
    lookupPipeline.push({
      $match: tokenQuery as unknown as Record<string, unknown>
    });
  }

  return getMongoCollections(params.database).erc1155Balances.aggregate<Erc1155BalanceDocument>([
    {
      $match: finalQuery
    },
    {
      $sort: {
        updatedAt: -1,
        _id: -1
      }
    },
    {
      $lookup: {
        from: mongoCollectionNames.tokens,
        let: {
          chainId: "$chainId",
          contractAddress: "$contractAddress",
          tokenId: "$tokenId"
        },
        pipeline: lookupPipeline,
        as: "matchingTokens"
      }
    },
    {
      $match: {
        matchingTokens: {
          $ne: []
        }
      }
    },
    {
      $project: {
        matchingTokens: 0
      }
    },
    {
      $limit: params.limit ?? 100
    }
  ]).toArray();
}

export async function listErc721OwnershipByOwner(params: {
  database: Db;
  chainId: number;
  ownerAddress: string;
  contractAddress?: string;
  metadataStatus?: MetadataStatus;
  mediaStatus?: MediaStatus;
  traitType?: string;
  traitValue?: string | number | boolean;
  queryText?: string;
  limit?: number;
  cursor?: {
    updatedAt: Date;
    id: ObjectId;
  };
}): Promise<Erc721OwnershipDocument[]> {
  const query: Filter<Erc721OwnershipDocument> = {
    chainId: params.chainId,
    ownerAddress: normalizeWalletAddress(params.ownerAddress)
  };

  if (params.contractAddress) {
    query.contractAddress = normalizeContractAddress(params.contractAddress);
  }

  const tokenFilterParams: {
    metadataStatus?: MetadataStatus;
    mediaStatus?: MediaStatus;
    traitType?: string;
    traitValue?: string | number | boolean;
    queryText?: string;
  } = {};

  if (params.metadataStatus) {
    tokenFilterParams.metadataStatus = params.metadataStatus;
  }

  if (params.mediaStatus) {
    tokenFilterParams.mediaStatus = params.mediaStatus;
  }

  if (params.traitType) {
    tokenFilterParams.traitType = params.traitType;
  }

  if (params.traitValue !== undefined) {
    tokenFilterParams.traitValue = params.traitValue;
  }

  if (params.queryText) {
    tokenFilterParams.queryText = params.queryText;
  }

  const tokenQuery = buildTokenFilterQuery(tokenFilterParams);

  const cursorQuery: Filter<Erc721OwnershipDocument> | null = params.cursor
    ? {
        $or: [
          {
            updatedAt: {
              $lt: params.cursor.updatedAt
            }
          },
          {
            updatedAt: params.cursor.updatedAt,
            _id: {
              $lt: params.cursor.id
            }
          }
        ]
      }
    : null;

  const finalQuery = cursorQuery
    ? ({
        $and: [query, cursorQuery]
      } satisfies Filter<Erc721OwnershipDocument>)
    : query;

  const lookupPipeline: Array<Record<string, unknown>> = [
    {
      $match: {
        $expr: {
          $and: [
            { $eq: ["$chainId", "$$chainId"] },
            { $eq: ["$contractAddress", "$$contractAddress"] },
            { $eq: ["$tokenId", "$$tokenId"] }
          ]
        }
      }
    }
  ];

  if (Object.keys(tokenQuery).length > 0) {
    lookupPipeline.push({
      $match: tokenQuery as unknown as Record<string, unknown>
    });
  }

  return getMongoCollections(params.database).erc721Ownership.aggregate<Erc721OwnershipDocument>([
    {
      $match: finalQuery
    },
    {
      $sort: {
        updatedAt: -1,
        _id: -1
      }
    },
    {
      $lookup: {
        from: mongoCollectionNames.tokens,
        let: {
          chainId: "$chainId",
          contractAddress: "$contractAddress",
          tokenId: "$tokenId"
        },
        pipeline: lookupPipeline,
        as: "matchingTokens"
      }
    },
    {
      $match: {
        matchingTokens: {
          $ne: []
        }
      }
    },
    {
      $project: {
        matchingTokens: 0
      }
    },
    {
      $limit: params.limit ?? 100
    }
  ]).toArray();
}

export async function listErc721OwnedTokenIdsForCollection(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
}): Promise<string[]> {
  return getMongoCollections(params.database).erc721Ownership.distinct("tokenId", {
    chainId: params.chainId,
    contractAddress: normalizeContractAddress(params.contractAddress)
  });
}

function buildTokenFilterQuery(params: {
  chainId?: number;
  contractAddress?: string;
  metadataStatus?: MetadataStatus;
  mediaStatus?: MediaStatus;
  traitType?: string;
  traitValue?: string | number | boolean;
  queryText?: string;
}): Filter<TokenDocument> {
  const query: Filter<TokenDocument> = {};

  if (params.chainId !== undefined) {
    query.chainId = params.chainId;
  }

  if (params.contractAddress) {
    query.contractAddress = normalizeContractAddress(params.contractAddress);
  }

  const searchQuery = buildTokenSearchQuery(params.queryText);

  if (searchQuery) {
    query.$or = searchQuery;
  }

  if (params.metadataStatus) {
    query.metadataStatus = params.metadataStatus;
  }

  if (params.mediaStatus) {
    query.mediaStatus = params.mediaStatus;
  }

  if (params.traitType && params.traitValue !== undefined) {
    query.attributes = {
      $elemMatch: {
        trait_type: params.traitType,
        value: params.traitValue
      }
    };
  } else if (params.traitType) {
    query["attributes.trait_type"] = params.traitType;
  } else if (params.traitValue !== undefined) {
    query["attributes.value"] = params.traitValue;
  }

  return query;
}

function buildTokenSearchQuery(queryText: string | undefined): Filter<TokenDocument>["$or"] | null {
  if (!queryText?.trim()) {
    return null;
  }

  const normalized = queryText.trim();
  const escaped = escapeRegExp(normalized);
  const containsPattern = new RegExp(escaped, "i");
  const tokenIdPattern = new RegExp(`^${escaped}$`, "i");
  const normalizedAddress = /^0x[a-fA-F0-9]{40}$/.test(normalized)
    ? normalizeContractAddress(normalized)
    : null;

  const clauses: Filter<TokenDocument>[] = [
    { name: containsPattern },
    { description: containsPattern },
    { tokenId: tokenIdPattern },
    {
      attributes: {
        $elemMatch: {
          $or: [
            { trait_type: containsPattern },
            { value: typeof normalized === "string" ? normalized : containsPattern }
          ]
        }
      }
    }
  ];

  if (normalizedAddress) {
    clauses.push({ contractAddress: normalizedAddress });
  }

  return clauses;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export async function findMediaAssetsByIds(params: {
  database: Db;
  assetIds: ObjectId[];
}): Promise<MediaAssetDocument[]> {
  if (params.assetIds.length === 0) {
    return [];
  }

  const uniqueAssetIds = [...new Map(params.assetIds.map((assetId) => [assetId.toHexString(), assetId])).values()];

  return getMongoCollections(params.database).mediaAssets.find({
    _id: { $in: uniqueAssetIds }
  }).toArray();
}

export async function upsertErc721Ownership(
  database: Db,
  document: Omit<Erc721OwnershipDocument, "_id">
): Promise<void> {
  await getMongoCollections(database).erc721Ownership.updateOne(
    {
      chainId: document.chainId,
      contractAddress: normalizeContractAddress(document.contractAddress),
      tokenId: document.tokenId
    },
    {
      $set: {
        chainId: document.chainId,
        contractAddress: normalizeContractAddress(document.contractAddress),
        tokenId: document.tokenId,
        ownerAddress: normalizeWalletAddress(document.ownerAddress),
        updatedAt: document.updatedAt
      }
    },
    { upsert: true }
  );
}

export async function findErc721OwnershipByToken(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
  tokenId: string;
}): Promise<Erc721OwnershipDocument | null> {
  return getMongoCollections(params.database).erc721Ownership.findOne({
    chainId: params.chainId,
    contractAddress: normalizeContractAddress(params.contractAddress),
    tokenId: params.tokenId
  });
}

export async function replaceErc721OwnershipForCollection(
  database: Db,
  params: {
    chainId: number;
    contractAddress: string;
    ownerships: Array<Omit<Erc721OwnershipDocument, "_id">>;
  }
): Promise<void> {
  const normalizedContractAddress = normalizeContractAddress(params.contractAddress);
  const normalizedOwnerships = params.ownerships.map((ownership) => ({
    chainId: ownership.chainId,
    contractAddress: normalizedContractAddress,
    tokenId: ownership.tokenId,
    ownerAddress: normalizeWalletAddress(ownership.ownerAddress),
    updatedAt: ownership.updatedAt
  }));
  const collection = getMongoCollections(database).erc721Ownership;

  if (normalizedOwnerships.length > 0) {
    await collection.bulkWrite(
      normalizedOwnerships.map((ownership) => ({
        replaceOne: {
          filter: {
            chainId: ownership.chainId,
            contractAddress: ownership.contractAddress,
            tokenId: ownership.tokenId
          },
          replacement: ownership,
          upsert: true
        }
      })),
      { ordered: false }
    );
  }

  const staleTokenFilters = normalizedOwnerships.map((ownership) => ({ tokenId: ownership.tokenId }));

  await collection.deleteMany(
    staleTokenFilters.length > 0
      ? {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress,
          $nor: staleTokenFilters
        }
      : {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress
        }
  );
}

export async function replaceErc1155BalancesForCollection(
  database: Db,
  params: {
    chainId: number;
    contractAddress: string;
    balances: Array<Omit<Erc1155BalanceDocument, "_id">>;
  }
): Promise<void> {
  const normalizedContractAddress = normalizeContractAddress(params.contractAddress);
  const normalizedBalances = params.balances
    .filter((balance) => BigInt(balance.balance) > 0n)
    .map((balance) => ({
      chainId: balance.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: balance.tokenId,
      ownerAddress: normalizeWalletAddress(balance.ownerAddress),
      balance: balance.balance,
      updatedAt: balance.updatedAt
    }));
  const collection = getMongoCollections(database).erc1155Balances;

  if (normalizedBalances.length > 0) {
    await collection.bulkWrite(
      normalizedBalances.map((balance) => ({
        replaceOne: {
          filter: {
            chainId: balance.chainId,
            contractAddress: balance.contractAddress,
            tokenId: balance.tokenId,
            ownerAddress: balance.ownerAddress
          },
          replacement: balance,
          upsert: true
        }
      })),
      { ordered: false }
    );
  }

  const staleFilters = normalizedBalances.map((balance) => ({
    tokenId: balance.tokenId,
    ownerAddress: balance.ownerAddress
  }));

  await collection.deleteMany(
    staleFilters.length > 0
      ? {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress,
          $nor: staleFilters
        }
      : {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress
        }
  );
}

export async function replaceErc1155BalancesForToken(
  database: Db,
  params: {
    chainId: number;
    contractAddress: string;
    tokenId: string;
    balances: Array<Omit<Erc1155BalanceDocument, "_id">>;
  }
): Promise<void> {
  const normalizedContractAddress = normalizeContractAddress(params.contractAddress);
  const normalizedBalances = params.balances
    .filter((balance) => BigInt(balance.balance) > 0n)
    .map((balance) => ({
      chainId: balance.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: params.tokenId,
      ownerAddress: normalizeWalletAddress(balance.ownerAddress),
      balance: balance.balance,
      updatedAt: balance.updatedAt
    }));
  const collection = getMongoCollections(database).erc1155Balances;

  if (normalizedBalances.length > 0) {
    await collection.bulkWrite(
      normalizedBalances.map((balance) => ({
        replaceOne: {
          filter: {
            chainId: balance.chainId,
            contractAddress: balance.contractAddress,
            tokenId: balance.tokenId,
            ownerAddress: balance.ownerAddress
          },
          replacement: balance,
          upsert: true
        }
      })),
      { ordered: false }
    );
  }

  const staleOwnerFilters = normalizedBalances.map((balance) => ({ ownerAddress: balance.ownerAddress }));

  await collection.deleteMany(
    staleOwnerFilters.length > 0
      ? {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress,
          tokenId: params.tokenId,
          $nor: staleOwnerFilters
        }
      : {
          chainId: params.chainId,
          contractAddress: normalizedContractAddress,
          tokenId: params.tokenId
        }
  );
}

export async function upsertToken(database: Db, document: Omit<TokenDocument, "_id">): Promise<void> {
  const { createdAt, ...updatableFields } = document;

  await getMongoCollections(database).tokens.updateOne(
    {
      chainId: document.chainId,
      contractAddress: normalizeContractAddress(document.contractAddress),
      tokenId: document.tokenId
    },
    {
      $set: {
        ...updatableFields,
        contractAddress: normalizeContractAddress(document.contractAddress),
        updatedAt: document.updatedAt
      },
      $setOnInsert: {
        createdAt
      }
    },
    { upsert: true }
  );
}

export async function deleteTokenAndDependents(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
  tokenId: string;
}): Promise<boolean> {
  const normalizedContractAddress = normalizeContractAddress(params.contractAddress);
  const collections = getMongoCollections(params.database);
  const token = await collections.tokens.findOne({
    chainId: params.chainId,
    contractAddress: normalizedContractAddress,
    tokenId: params.tokenId
  });

  await Promise.all([
    collections.erc721Ownership.deleteMany({
      chainId: params.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: params.tokenId
    }),
    collections.erc1155Balances.deleteMany({
      chainId: params.chainId,
      contractAddress: normalizedContractAddress,
      tokenId: params.tokenId
    }),
    token
      ? collections.metadataVersions.deleteMany({ tokenRef: token._id })
      : Promise.resolve(),
    token
      ? collections.mediaAssets.deleteMany({ tokenRef: token._id })
      : Promise.resolve(),
    token
      ? collections.tokens.deleteOne({ _id: token._id })
      : Promise.resolve()
  ]);

  return Boolean(token);
}

export async function createJob(database: Db, input: Omit<JobDocument, "_id">): Promise<ObjectId> {
  if (input.queueJobId) {
    const document = await getMongoCollections(database).jobs.findOneAndUpdate(
      { queueJobId: input.queueJobId },
      {
        $set: {
          queueJobId: input.queueJobId,
          type: input.type,
          payload: input.payload,
          status: input.status,
          attempts: input.attempts,
          lastError: input.lastError,
          updatedAt: input.updatedAt
        },
        $setOnInsert: {
          createdAt: input.createdAt
        }
      },
      {
        upsert: true,
        returnDocument: "after"
      }
    );

    if (!document) {
      throw new Error("Failed to upsert queue-backed job document.");
    }

    return document._id;
  }

  const result = await getMongoCollections(database).jobs.insertOne(
    input as OptionalUnlessRequiredId<JobDocument>
  );
  return result.insertedId;
}

export async function findJobByQueueJobId(params: {
  database: Db;
  queueJobId: string;
}): Promise<JobDocument | null> {
  return getMongoCollections(params.database).jobs.findOne({ queueJobId: params.queueJobId });
}

export async function upsertQueueBackedJobState(database: Db, input: {
  queueJobId: string;
  type: JobType;
  payload: Record<string, unknown>;
  status: JobStatus;
  attempts: number;
  lastError: string | null;
  updatedAt: Date;
}): Promise<void> {
  await getMongoCollections(database).jobs.updateOne(
    { queueJobId: input.queueJobId },
    {
      $set: {
        queueJobId: input.queueJobId,
        type: input.type,
        payload: input.payload,
        status: input.status,
        attempts: input.attempts,
        lastError: input.lastError,
        updatedAt: input.updatedAt
      },
      $setOnInsert: {
        createdAt: input.updatedAt
      }
    },
    { upsert: true }
  );
}

export async function findApiClientByKeyHash(params: {
  database: Db;
  keyHash: string;
}): Promise<ApiClientDocument | null> {
  return getMongoCollections(params.database).apiClients.findOne({ keyHash: params.keyHash });
}

export async function findApiClientByClientId(params: {
  database: Db;
  clientId: string;
}): Promise<ApiClientDocument | null> {
  return getMongoCollections(params.database).apiClients.findOne({ clientId: params.clientId });
}

export async function upsertApiClient(
  database: Db,
  document: Omit<ApiClientDocument, "_id">
): Promise<void> {
  const { createdAt, ...updatableFields } = document;

  await getMongoCollections(database).apiClients.updateOne(
    { clientId: document.clientId },
    {
      $set: {
        ...updatableFields,
        updatedAt: document.updatedAt
      },
      $setOnInsert: {
        createdAt
      }
    },
    { upsert: true }
  );
}

export async function markApiClientUsed(params: {
  database: Db;
  clientId: string;
  usedAt: Date;
}): Promise<void> {
  await getMongoCollections(params.database).apiClients.updateOne(
    { clientId: params.clientId },
    {
      $set: {
        lastUsedAt: params.usedAt,
        updatedAt: params.usedAt
      }
    }
  );
}

export async function insertAuditLog(
  database: Db,
  document: Omit<AuditLogDocument, "_id">
): Promise<void> {
  await getMongoCollections(database).auditLogs.insertOne(
    document as OptionalUnlessRequiredId<AuditLogDocument>
  );
}

export async function upsertMediaAsset(
  database: Db,
  document: Omit<MediaAssetDocument, "_id">
): Promise<MediaAssetDocument> {
  const { createdAt, ...updatableFields } = document;
  const result = await getMongoCollections(database).mediaAssets.findOneAndUpdate(
    {
      tokenRef: document.tokenRef,
      kind: document.kind
    },
    {
      $set: {
        ...updatableFields,
        updatedAt: document.updatedAt
      },
      $setOnInsert: {
        createdAt
      }
    },
    {
      upsert: true,
      returnDocument: "after"
    }
  );

  if (!result) {
    throw new Error("Failed to upsert media asset document.");
  }

  return result;
}

export async function createMetadataVersion(
  database: Db,
  document: Omit<MetadataVersionDocument, "_id">
): Promise<ObjectId> {
  const result = await getMongoCollections(database).metadataVersions.insertOne(
    document as OptionalUnlessRequiredId<MetadataVersionDocument>
  );

  return result.insertedId;
}

export async function ensureBootstrapApiClient(
  database: Db,
  input: BootstrapApiClientSeed
): Promise<void> {
  const timestamp = new Date();

  await upsertApiClient(database, {
    clientId: input.clientId,
    clientName: input.clientName,
    keyPrefix: buildApiKeyPrefix(input.apiKey),
    keyHash: sha256Hex(input.apiKey),
    secretEncrypted: encryptSecret({
      plaintext: input.apiSecret,
      encryptionKey: input.encryptionKey
    }),
    scopes: input.scopes,
    rateLimitPerMinute: input.rateLimitPerMinute,
    allowedIps: input.allowedIps,
    status: "active",
    lastUsedAt: null,
    createdAt: timestamp,
    updatedAt: timestamp
  });
}

export function serializeObjectId(value: ObjectId | null | undefined): string | null {
  return value ? value.toHexString() : null;
}

export function serializeCollectionDocument(document: CollectionDocument) {
  return {
    ...document,
    _id: document._id.toHexString()
  };
}

export function serializeTokenDocument(document: TokenDocument) {
  return {
    ...document,
    _id: document._id.toHexString(),
    imageAssetId: serializeObjectId(document.imageAssetId),
    animationAssetId: serializeObjectId(document.animationAssetId),
    audioAssetId: serializeObjectId(document.audioAssetId)
  };
}


export function serializeErc721OwnershipDocument(document: Erc721OwnershipDocument) {
  return {
    ...document,
    _id: document._id.toHexString()
  };
}

export function serializeErc1155BalanceDocument(document: Erc1155BalanceDocument) {
  return {
    ...document,
    _id: document._id.toHexString()
  };
}

export function serializeMediaAssetDocument(document: MediaAssetDocument) {
  return {
    ...document,
    _id: document._id.toHexString(),
    tokenRef: document.tokenRef.toHexString()
  };
}

export function serializeJobDocument(document: JobDocument) {
  return {
    ...document,
    _id: document._id.toHexString()
  };
}
