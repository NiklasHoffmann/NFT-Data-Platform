import { z } from "zod";

export const supportedChains = [
  { id: 1, key: "ethereum-mainnet", name: "Ethereum Mainnet" },
  { id: 11155111, key: "sepolia", name: "Sepolia" }
] as const;

export const supportedChainIds = supportedChains.map((chain) => chain.id);

export type SupportedChainId = (typeof supportedChains)[number]["id"];

export const nftStandardSchema = z.enum(["erc721", "erc1155"]);
export type NftStandard = z.infer<typeof nftStandardSchema>;

export const scopeSchema = z.enum([
  "collections:read",
  "tokens:read",
  "owners:read",
  "search:read",
  "refresh:token",
  "refresh:collection",
  "refresh:media",
  "reindex:write",
  "admin:read"
]);
export type Scope = z.infer<typeof scopeSchema>;

export const syncStatusSchema = z.enum(["pending", "syncing", "active", "error", "disabled"]);
export type SyncStatus = z.infer<typeof syncStatusSchema>;

export const metadataStatusSchema = z.enum(["pending", "ok", "failed", "stale"]);
export type MetadataStatus = z.infer<typeof metadataStatusSchema>;

export const mediaStatusSchema = z.enum(["pending", "processing", "ready", "partial", "failed"]);
export type MediaStatus = z.infer<typeof mediaStatusSchema>;

export const jobTypeSchema = z.enum([
  "refresh-token",
  "refresh-collection",
  "refresh-media",
  "reindex-range"
]);
export type JobType = z.infer<typeof jobTypeSchema>;

export const jobStatusSchema = z.enum(["queued", "running", "done", "failed"]);
export type JobStatus = z.infer<typeof jobStatusSchema>;

export const apiClientStatusSchema = z.enum(["active", "disabled", "blocked"]);
export type ApiClientStatus = z.infer<typeof apiClientStatusSchema>;

export const mediaKindSchema = z.enum(["image", "video", "audio", "animation"]);
export type MediaKind = z.infer<typeof mediaKindSchema>;

export const interactiveMediaTypeSchema = z.enum(["html", "youtube", "interactive"]);
export type InteractiveMediaType = z.infer<typeof interactiveMediaTypeSchema>;

export const auditRateLimitDecisionSchema = z.enum(["allow", "deny"]);
export type AuditRateLimitDecision = z.infer<typeof auditRateLimitDecisionSchema>;

export const evmAddressSchema = z
  .string()
  .trim()
  .regex(/^0x[a-fA-F0-9]{40}$/, "Expected a valid EVM address.");

export const walletAddressSchema = evmAddressSchema;

export const tokenIdentitySchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: evmAddressSchema,
  tokenId: z.string().min(1)
});

export type TokenIdentity = z.infer<typeof tokenIdentitySchema>;

export const collectionIdentitySchema = z.object({
  chainId: z.number().int().positive(),
  contractAddress: evmAddressSchema
});

export type CollectionIdentity = z.infer<typeof collectionIdentitySchema>;

export const attributeValueSchema = z.union([z.string(), z.number(), z.boolean()]);

export const tokenAttributeSchema = z.object({
  trait_type: z.string().min(1).optional(),
  value: attributeValueSchema.optional(),
  display_type: z.string().min(1).optional()
});

export type TokenAttribute = z.infer<typeof tokenAttributeSchema>;

export const collectionRecordSchema = collectionIdentitySchema.extend({
  standard: nftStandardSchema,
  name: z.string().min(1).nullable(),
  symbol: z.string().min(1).nullable(),
  baseUri: z.string().min(1).nullable(),
  contractUriRaw: z.string().min(1).nullable(),
  contractUriResolved: z.string().min(1).nullable(),
  creatorName: z.string().min(1).nullable(),
  creatorAddress: walletAddressSchema.nullable(),
  contractOwnerAddress: walletAddressSchema.nullable(),
  royaltyRecipientAddress: walletAddressSchema.nullable(),
  royaltyBasisPoints: z.number().int().nonnegative().nullable(),
  collectionMetadataPayload: z.record(z.string(), z.unknown()).nullable(),
  collectionMetadataHash: z.string().min(1).nullable(),
  lastCollectionMetadataFetchAt: z.date().nullable(),
  lastCollectionMetadataError: z.string().min(1).nullable(),
  description: z.string().min(1).nullable(),
  externalUrl: z.string().url().nullable(),
  imageOriginalUrl: z.string().min(1).nullable(),
  bannerImageOriginalUrl: z.string().min(1).nullable(),
  featuredImageOriginalUrl: z.string().min(1).nullable(),
  animationOriginalUrl: z.string().min(1).nullable(),
  audioOriginalUrl: z.string().min(1).nullable(),
  interactiveOriginalUrl: z.string().min(1).nullable(),
  totalSupply: z.string().min(1).nullable(),
  indexedTokenCount: z.number().int().nonnegative(),
  deployBlock: z.number().int().nonnegative().nullable(),
  lastObservedBlock: z.number().int().nonnegative().nullable(),
  lastIndexedBlock: z.number().int().nonnegative().nullable(),
  syncStatus: syncStatusSchema,
  lastSyncAt: z.date().nullable(),
  createdAt: z.date(),
  updatedAt: z.date()
});

export type CollectionRecord = z.infer<typeof collectionRecordSchema>;

export const tokenRecordSchema = tokenIdentitySchema.extend({
  standard: nftStandardSchema,
  metadataUriRaw: z.string().min(1).nullable(),
  metadataUriResolved: z.string().min(1).nullable(),
  supplyQuantity: z.string().min(1).nullable(),
  metadataStatus: metadataStatusSchema,
  metadataVersion: z.number().int().nonnegative(),
  metadataHash: z.string().min(1).nullable(),
  metadataPayload: z.record(z.string(), z.unknown()).nullable(),
  lastMetadataError: z.string().min(1).nullable(),
  name: z.string().min(1).nullable(),
  description: z.string().min(1).nullable(),
  externalUrl: z.string().url().nullable(),
  imageOriginalUrl: z.string().min(1).nullable(),
  imageAssetId: z.string().min(1).nullable(),
  animationOriginalUrl: z.string().min(1).nullable(),
  animationAssetId: z.string().min(1).nullable(),
  audioOriginalUrl: z.string().min(1).nullable(),
  audioAssetId: z.string().min(1).nullable(),
  interactiveOriginalUrl: z.string().min(1).nullable(),
  interactiveMediaType: interactiveMediaTypeSchema.nullable(),
  attributes: z.array(tokenAttributeSchema),
  mediaStatus: mediaStatusSchema,
  ownerStateVersion: z.number().int().nonnegative(),
  lastMetadataFetchAt: z.date().nullable(),
  lastMediaProcessAt: z.date().nullable(),
  createdAt: z.date(),
  updatedAt: z.date()
});

export type TokenRecord = z.infer<typeof tokenRecordSchema>;

export const erc721OwnershipRecordSchema = tokenIdentitySchema.extend({
  ownerAddress: walletAddressSchema,
  updatedAt: z.date()
});

export type Erc721OwnershipRecord = z.infer<typeof erc721OwnershipRecordSchema>;

export const erc1155BalanceRecordSchema = tokenIdentitySchema.extend({
  ownerAddress: walletAddressSchema,
  balance: z.string().min(1),
  updatedAt: z.date()
});

export type Erc1155BalanceRecord = z.infer<typeof erc1155BalanceRecordSchema>;

export const metadataVersionRecordSchema = z.object({
  tokenRef: z.string().min(1),
  version: z.number().int().positive(),
  sourceUri: z.string().min(1),
  payload: z.record(z.string(), z.unknown()),
  payloadHash: z.string().min(1),
  fetchedAt: z.date()
});

export type MetadataVersionRecord = z.infer<typeof metadataVersionRecordSchema>;

export const mediaAssetRecordSchema = z.object({
  tokenRef: z.string().min(1),
  kind: mediaKindSchema,
  sourceUrl: z.string().min(1),
  storageKeyOriginal: z.string().min(1),
  storageKeyOptimized: z.string().min(1).nullable(),
  storageKeyThumbnail: z.string().min(1).nullable(),
  cdnUrlOriginal: z.string().url().nullable(),
  cdnUrlOptimized: z.string().url().nullable(),
  cdnUrlThumbnail: z.string().url().nullable(),
  mimeType: z.string().min(1).nullable(),
  sizeBytes: z.number().int().nonnegative().nullable(),
  checksumSha256: z.string().min(1).nullable(),
  width: z.number().int().positive().nullable(),
  height: z.number().int().positive().nullable(),
  durationSec: z.number().nonnegative().nullable(),
  status: mediaStatusSchema,
  statusDetail: z.string().min(1).nullable(),
  createdAt: z.date(),
  updatedAt: z.date()
});

export type MediaAssetRecord = z.infer<typeof mediaAssetRecordSchema>;

export const jobRecordSchema = z.object({
  queueJobId: z.string().min(1).nullable(),
  type: jobTypeSchema,
  payload: z.record(z.string(), z.unknown()),
  status: jobStatusSchema,
  attempts: z.number().int().nonnegative(),
  lastError: z.string().min(1).nullable(),
  createdAt: z.date(),
  updatedAt: z.date()
});

export type JobRecord = z.infer<typeof jobRecordSchema>;

export const apiClientRecordSchema = z.object({
  clientId: z.string().min(1),
  clientName: z.string().min(1),
  keyPrefix: z.string().min(1),
  keyHash: z.string().min(1),
  secretEncrypted: z.string().min(1).nullable(),
  scopes: z.array(scopeSchema),
  rateLimitPerMinute: z.number().int().positive(),
  allowedIps: z.array(z.string()),
  status: apiClientStatusSchema,
  lastUsedAt: z.date().nullable(),
  createdAt: z.date(),
  updatedAt: z.date()
});

export type ApiClientRecord = z.infer<typeof apiClientRecordSchema>;

export const auditLogRecordSchema = z.object({
  clientId: z.string().min(1),
  scope: scopeSchema.nullable(),
  method: z.string().min(1),
  path: z.string().min(1),
  statusCode: z.number().int().min(100).max(599),
  responseTimeMs: z.number().int().nonnegative(),
  ip: z.string().min(1).nullable(),
  timestamp: z.date(),
  rateLimitDecision: auditRateLimitDecisionSchema
});

export type AuditLogRecord = z.infer<typeof auditLogRecordSchema>;

export function normalizeAddress(address: string): string {
  return address.trim().toLowerCase();
}

export function normalizeContractAddress(address: string): string {
  return normalizeAddress(address);
}

export function normalizeWalletAddress(address: string): string {
  return normalizeAddress(address);
}
