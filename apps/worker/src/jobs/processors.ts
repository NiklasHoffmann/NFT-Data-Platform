import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import { Queue, type Job } from "bullmq";
import type IORedis from "ioredis";
import type { Db, ObjectId } from "mongodb";
import {
  detectNftStandard,
  findContractDeploymentBlock,
  getChainPublicClient,
  getRpcUrlForChain,
  hasErc1155TokenTransferActivity,
  hasContractBytecode,
  isSupportedChainId,
  normalizeAssetUri,
  readCollectionSignalsOnChain,
  readErc721TransfersInRange,
  readErc1155TokenExists,
  readErc1155TransfersForTokenInRange,
  readErc1155TransfersInRange,
  readCollectionOnChain,
  readTokenOnChain
} from "@nft-platform/chain";
import { normalizeWalletAddress, type InteractiveMediaType, type TokenAttribute } from "@nft-platform/domain";
import {
  type CollectionDocument,
  countTokensForCollections,
  createMetadataVersion,
  deleteTokenAndDependents,
  findCollectionByIdentity,
  listTokens,
  listErc1155Balances,
  findTokenByIdentity,
  replaceErc721OwnershipForCollection,
  replaceErc1155BalancesForCollection,
  replaceErc1155BalancesForToken,
  upsertErc721Ownership,
  upsertCollection,
  upsertMediaAsset,
  upsertQueueBackedJobState,
  upsertToken
} from "@nft-platform/db";
import { queueNames, type QueueName } from "@nft-platform/queue";
import {
  buildQueueAddOptions,
  refreshCollectionJobSchema,
  refreshMediaJobSchema,
  refreshTokenJobSchema,
  reindexRangeJobSchema,
  type RefreshCollectionJob,
  type RefreshMediaJob,
  type RefreshTokenJob,
  type ReindexRangeJob
} from "@nft-platform/queue";
import {
  buildImageDerivatives,
  buildMediaObjectKeys,
  createStorageClient,
  type StorageConfig,
  uploadStorageObject
} from "@nft-platform/storage";

type JobProcessingContext = {
  database: Db;
  redisConnection: IORedis;
  rpcMainnetUrl: string;
  rpcSepoliaUrl: string;
  storage: StorageConfig;
  mediaMaxVideoBytes: number;
};

export async function processQueueJob(params: {
  queueName: QueueName;
  job: Job;
  context: JobProcessingContext;
}): Promise<Record<string, unknown>> {
  const queueJobId = params.job.id;

  if (!queueJobId) {
    throw new Error("BullMQ job is missing a stable job id.");
  }

  const timestamp = new Date();
  await upsertQueueBackedJobState(params.context.database, {
    queueJobId,
    type: params.queueName,
    payload: params.job.data as Record<string, unknown>,
    status: "running",
    attempts: params.job.attemptsMade + 1,
    lastError: null,
    updatedAt: timestamp
  });

  try {
    const result = await dispatchJob(params.queueName, params.job, params.context);

    await upsertQueueBackedJobState(params.context.database, {
      queueJobId,
      type: params.queueName,
      payload: params.job.data as Record<string, unknown>,
      status: "done",
      attempts: params.job.attemptsMade + 1,
      lastError: null,
      updatedAt: new Date()
    });

    return result;
  } catch (error) {
    const shouldRetry = isRetryableQueueError(error) && hasRemainingAttempts(params.job);

    await upsertQueueBackedJobState(params.context.database, {
      queueJobId,
      type: params.queueName,
      payload: params.job.data as Record<string, unknown>,
      status: shouldRetry ? "queued" : "failed",
      attempts: params.job.attemptsMade + 1,
      lastError: error instanceof Error ? error.message : String(error),
      updatedAt: new Date()
    });

    throw error;
  }
}

async function dispatchJob(
  queueName: QueueName,
  job: Job,
  context: JobProcessingContext
): Promise<Record<string, unknown>> {
  switch (queueName) {
    case queueNames.refreshCollection:
      return handleRefreshCollection(refreshCollectionJobSchema.parse(job.data), context);
    case queueNames.refreshToken:
      return handleRefreshToken(refreshTokenJobSchema.parse(job.data), context);
    case queueNames.refreshMedia:
      return handleRefreshMedia(refreshMediaJobSchema.parse(job.data), context, job);
    case queueNames.reindexRange:
      return handleReindexRange(reindexRangeJobSchema.parse(job.data), context);
    default:
      throw new Error(`Unsupported queue name: ${queueName}`);
  }
}

function hasRemainingAttempts(job: Job): boolean {
  const configuredAttempts = typeof job.opts.attempts === "number" && job.opts.attempts > 0 ? job.opts.attempts : 1;
  return job.attemptsMade + 1 < configuredAttempts;
}

function isRetryableQueueError(error: unknown): boolean {
  return error instanceof RetryableQueueError;
}

class RetryableQueueError extends Error {
  readonly retryable = true;

  constructor(message: string) {
    super(message);
    this.name = "RetryableQueueError";
  }
}

async function handleRefreshCollection(
  payload: RefreshCollectionJob,
  context: JobProcessingContext
): Promise<Record<string, unknown>> {
  const existingCollection = await findCollectionByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress
  });
  const collection = await ensureCollectionRegistration(payload, context, existingCollection);

  return {
    collectionUpdated: true,
    collectionCreated: !existingCollection,
    syncStatus: collection.syncStatus,
    standard: collection.standard,
    deployBlock: collection.deployBlock,
    latestBlock: collection.lastObservedBlock
  };
}

async function ensureCollectionRegistration(
  payload: RefreshCollectionJob,
  context: JobProcessingContext,
  existingCollection?: CollectionDocument | null
): Promise<CollectionDocument> {
  assertSupportedChain(payload.chainId);
  const now = new Date();
  const publicClient = getPublicClientForChain(payload.chainId, context);
  const currentCollection =
    existingCollection ??
    (await findCollectionByIdentity({
      database: context.database,
      chainId: payload.chainId,
      contractAddress: payload.contractAddress
    }));

  const contractHasBytecode = await hasContractBytecode({
    client: publicClient,
    contractAddress: payload.contractAddress
  });

  if (!contractHasBytecode) {
    throw new Error("Address has no deployed contract bytecode on the selected chain.");
  }

  const standard =
    currentCollection?.standard ??
    payload.standard ??
    (await detectNftStandard(
      payload.tokenIdHint
        ? {
            client: publicClient,
            contractAddress: payload.contractAddress,
            tokenIdHint: payload.tokenIdHint
          }
        : {
            client: publicClient,
            contractAddress: payload.contractAddress
          }
    ));

  if (!standard) {
    throw new Error("Unable to detect NFT standard for the collection contract.");
  }

  const [onChainCollection, onChainSignals] = await Promise.all([
    readCollectionOnChain({
      client: publicClient,
      contractAddress: payload.contractAddress,
      standard
    }),
    readCollectionSignalsOnChain({
      client: publicClient,
      contractAddress: payload.contractAddress,
      royaltyTokenIdHint: payload.tokenIdHint ?? null
    })
  ]);
  let collectionMetadata: Awaited<ReturnType<typeof resolveCollectionMetadata>> = null;
  let collectionMetadataError: string | null = null;

  try {
    collectionMetadata = await resolveCollectionMetadata({
      rawUri: onChainCollection.contractUriRaw,
      resolvedUri: onChainCollection.contractUriResolved
    });
  } catch (error) {
    collectionMetadata = null;
    collectionMetadataError = error instanceof Error ? error.message : String(error);
  }
  const deployBlock =
    currentCollection?.deployBlock ??
    (await findContractDeploymentBlock({
      client: publicClient,
      contractAddress: payload.contractAddress,
      latestBlock: onChainCollection.latestBlock
    }));
  const resetIndexedCheckpoint = payload.fullRescan === true;

  await upsertCollection(context.database, {
    chainId: payload.chainId,
    contractAddress: payload.contractAddress,
    standard,
    name: collectionMetadata?.name ?? onChainCollection.name ?? currentCollection?.name ?? null,
    symbol: onChainCollection.symbol ?? currentCollection?.symbol ?? null,
    baseUri: currentCollection?.baseUri ?? null,
    contractUriRaw: onChainCollection.contractUriRaw ?? currentCollection?.contractUriRaw ?? null,
    contractUriResolved: onChainCollection.contractUriResolved ?? currentCollection?.contractUriResolved ?? null,
    creatorName: collectionMetadata?.creatorName ?? currentCollection?.creatorName ?? null,
    creatorAddress: collectionMetadata?.creatorAddress ?? currentCollection?.creatorAddress ?? null,
    contractOwnerAddress: onChainSignals.contractOwnerAddress ?? currentCollection?.contractOwnerAddress ?? null,
    royaltyRecipientAddress:
      onChainSignals.royaltyRecipientAddress ??
      collectionMetadata?.royaltyRecipientAddress ??
      currentCollection?.royaltyRecipientAddress ??
      null,
    royaltyBasisPoints:
      onChainSignals.royaltyBasisPoints ?? collectionMetadata?.royaltyBasisPoints ?? currentCollection?.royaltyBasisPoints ?? null,
    collectionMetadataPayload: collectionMetadata?.payload ?? currentCollection?.collectionMetadataPayload ?? null,
    collectionMetadataHash: collectionMetadata?.payloadHash ?? currentCollection?.collectionMetadataHash ?? null,
    lastCollectionMetadataFetchAt:
      collectionMetadata ? now : currentCollection?.lastCollectionMetadataFetchAt ?? null,
    lastCollectionMetadataError:
      onChainCollection.contractUriResolved && !collectionMetadata
        ? collectionMetadataError ?? currentCollection?.lastCollectionMetadataError ?? "Collection metadata could not be resolved."
        : null,
    description: collectionMetadata?.description ?? currentCollection?.description ?? null,
    externalUrl: collectionMetadata?.externalUrl ?? currentCollection?.externalUrl ?? null,
    imageOriginalUrl: collectionMetadata?.imageOriginalUrl ?? currentCollection?.imageOriginalUrl ?? null,
    bannerImageOriginalUrl:
      collectionMetadata?.bannerImageOriginalUrl ?? currentCollection?.bannerImageOriginalUrl ?? null,
    featuredImageOriginalUrl:
      collectionMetadata?.featuredImageOriginalUrl ?? currentCollection?.featuredImageOriginalUrl ?? null,
    animationOriginalUrl: collectionMetadata?.animationOriginalUrl ?? currentCollection?.animationOriginalUrl ?? null,
    audioOriginalUrl: collectionMetadata?.audioOriginalUrl ?? currentCollection?.audioOriginalUrl ?? null,
    interactiveOriginalUrl:
      collectionMetadata?.interactiveOriginalUrl ?? currentCollection?.interactiveOriginalUrl ?? null,
    totalSupply: onChainCollection.totalSupply ?? currentCollection?.totalSupply ?? null,
    indexedTokenCount: currentCollection?.indexedTokenCount ?? 0,
    deployBlock,
    lastObservedBlock: onChainCollection.latestBlock ?? currentCollection?.lastObservedBlock ?? null,
    lastIndexedBlock: resetIndexedCheckpoint ? null : currentCollection?.lastIndexedBlock ?? null,
    syncStatus: "active",
    lastSyncAt: now,
    createdAt: currentCollection?.createdAt ?? now,
    updatedAt: now
  });

  const registeredCollection = await findCollectionByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress
  });

  if (!registeredCollection) {
    throw new Error("Collection refresh upsert did not persist the collection document.");
  }

  return registeredCollection;
}

async function handleRefreshToken(
  payload: RefreshTokenJob,
  context: JobProcessingContext
): Promise<Record<string, unknown>> {
  assertSupportedChain(payload.chainId);
  const now = new Date();
  const publicClient = getPublicClientForChain(payload.chainId, context);
  const collection =
    (await findCollectionByIdentity({
      database: context.database,
      chainId: payload.chainId,
      contractAddress: payload.contractAddress
    })) ??
    (await ensureCollectionRegistration(
      {
        chainId: payload.chainId,
        contractAddress: payload.contractAddress,
        tokenIdHint: payload.tokenId,
        fullRescan: false
      },
      context
    ));

  const onChainToken = await readTokenOnChain({
    client: publicClient,
    contractAddress: payload.contractAddress,
    standard: collection.standard,
    tokenId: payload.tokenId
  });

  const existingToken = await findTokenByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress,
    tokenId: payload.tokenId
  });

  const hasOnChainErc1155Uri =
    collection.standard === "erc1155" &&
    Boolean(onChainToken.metadataUriRaw || onChainToken.metadataUriResolved);

  const erc1155TokenExists =
    collection.standard === "erc1155"
      ? await detectErc1155TokenExistence({
          payload,
          collection,
          context,
          publicClient,
          onChainToken
        })
      : null;

  const shouldMaterializeErc1155Balances =
    collection.standard === "erc1155" &&
    (payload.forceOwnership ||
      (onChainToken.supplyQuantity === null && (hasOnChainErc1155Uri || erc1155TokenExists === true)));

  const erc1155BalanceSnapshot = shouldMaterializeErc1155Balances
    ? await materializeErc1155BalancesForToken({
        chainId: payload.chainId,
        contractAddress: payload.contractAddress,
        tokenId: payload.tokenId,
        collection,
        context,
        updatedAt: now,
        publicClient
      })
    : null;

  if (collection.standard === "erc721" && !onChainToken.ownerAddress) {
    await removeMissingTokenFromReadModel({
      collection,
      existingToken,
      context,
      updatedAt: now
    });

    throw new Error("Token not found on chain.");
  }

  if (collection.standard === "erc1155" && erc1155TokenExists !== true) {
    await removeMissingTokenFromReadModel({
      collection,
      existingToken,
      context,
      updatedAt: now
    });

    throw new Error("Token not found on chain.");
  }

  let metadata: Awaited<ReturnType<typeof resolveMetadata>> = null;
  let metadataError: string | null = null;

  try {
    metadata = await resolveMetadata({
      rawUri: onChainToken.metadataUriRaw,
      resolvedUri: onChainToken.metadataUriResolved
    });
  } catch (error) {
    metadataError = error instanceof Error ? error.message : String(error);
  }

  const metadataHashChanged = Boolean(
    metadata?.payloadHash && metadata.payloadHash !== existingToken?.metadataHash
  );
  const nextMetadataVersion = metadataHashChanged
    ? (existingToken?.metadataVersion ?? 0) + 1
    : existingToken?.metadataVersion ?? 0;
  const nextMetadataStatus = metadata
    ? "ok"
    : metadataError
      ? "failed"
      : onChainToken.metadataUriResolved
        ? existingToken?.metadataStatus ?? "pending"
      : existingToken?.metadataStatus ?? "pending";
  const nextMediaStatus = metadataHashChanged || payload.forceMetadata
    ? metadata?.hasDownloadableMedia
      ? "pending"
      : metadata?.hasMedia
        ? "ready"
      : metadataError
        ? existingToken?.mediaStatus ?? "failed"
        : existingToken?.mediaStatus ?? "pending"
    : existingToken?.mediaStatus ?? "pending";
  const nextOwnerStateVersion =
    (existingToken?.ownerStateVersion ?? 0) +
    (payload.forceOwnership || Boolean(onChainToken.ownerAddress) ? 1 : 0);

  await upsertToken(context.database, {
    chainId: payload.chainId,
    contractAddress: payload.contractAddress,
    tokenId: payload.tokenId,
    standard: existingToken?.standard ?? collection.standard,
    metadataUriRaw: onChainToken.metadataUriRaw,
    metadataUriResolved: onChainToken.metadataUriResolved,
    supplyQuantity:
      onChainToken.supplyQuantity ?? erc1155BalanceSnapshot?.totalQuantity ?? existingToken?.supplyQuantity ?? null,
    metadataStatus: nextMetadataStatus,
    metadataVersion: nextMetadataVersion,
    metadataHash: metadata?.payloadHash ?? existingToken?.metadataHash ?? null,
    metadataPayload: metadata?.payload ?? existingToken?.metadataPayload ?? null,
    lastMetadataError: metadata ? null : metadataError ?? existingToken?.lastMetadataError ?? null,
    name: metadata?.name ?? existingToken?.name ?? null,
    description: metadata?.description ?? existingToken?.description ?? null,
    externalUrl: metadata?.externalUrl ?? existingToken?.externalUrl ?? null,
    imageOriginalUrl: metadata?.imageOriginalUrl ?? existingToken?.imageOriginalUrl ?? null,
    imageAssetId: resolveNextMediaAssetId({
      existingAssetId: existingToken?.imageAssetId ?? null,
      existingSourceUrl: existingToken?.imageOriginalUrl ?? null,
      nextSourceUrl: metadata?.imageOriginalUrl ?? existingToken?.imageOriginalUrl ?? null,
      shouldReconcile: metadataHashChanged || payload.forceMetadata
    }),
    animationOriginalUrl: metadata?.animationOriginalUrl ?? existingToken?.animationOriginalUrl ?? null,
    animationAssetId: resolveNextMediaAssetId({
      existingAssetId: existingToken?.animationAssetId ?? null,
      existingSourceUrl: existingToken?.animationOriginalUrl ?? null,
      nextSourceUrl: metadata?.animationOriginalUrl ?? existingToken?.animationOriginalUrl ?? null,
      shouldReconcile: metadataHashChanged || payload.forceMetadata
    }),
    audioOriginalUrl: metadata?.audioOriginalUrl ?? existingToken?.audioOriginalUrl ?? null,
    audioAssetId: resolveNextMediaAssetId({
      existingAssetId: existingToken?.audioAssetId ?? null,
      existingSourceUrl: existingToken?.audioOriginalUrl ?? null,
      nextSourceUrl: metadata?.audioOriginalUrl ?? existingToken?.audioOriginalUrl ?? null,
      shouldReconcile: metadataHashChanged || payload.forceMetadata
    }),
    interactiveOriginalUrl: metadata?.interactiveOriginalUrl ?? existingToken?.interactiveOriginalUrl ?? null,
    interactiveMediaType: metadata?.interactiveMediaType ?? existingToken?.interactiveMediaType ?? null,
    attributes: metadata?.attributes ?? existingToken?.attributes ?? [],
    mediaStatus: nextMediaStatus,
    ownerStateVersion: nextOwnerStateVersion,
    lastMetadataFetchAt: metadata || metadataError ? now : existingToken?.lastMetadataFetchAt ?? null,
    lastMediaProcessAt: existingToken?.lastMediaProcessAt ?? null,
    createdAt: existingToken?.createdAt ?? now,
    updatedAt: now
  });

  const persistedToken = await findTokenByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress,
    tokenId: payload.tokenId
  });

  if (!persistedToken) {
    throw new Error("Token refresh upsert did not persist the token document.");
  }

  if (metadataHashChanged && metadata) {
    await createMetadataVersion(context.database, {
      tokenRef: persistedToken._id,
      version: nextMetadataVersion,
      sourceUri: onChainToken.metadataUriResolved ?? onChainToken.metadataUriRaw ?? "unknown",
      payload: metadata.payload,
      payloadHash: metadata.payloadHash,
      fetchedAt: now
    });
  }

  if (collection.standard === "erc721" && onChainToken.ownerAddress) {
    await upsertErc721Ownership(context.database, {
      chainId: payload.chainId,
      contractAddress: payload.contractAddress,
      tokenId: payload.tokenId,
      ownerAddress: onChainToken.ownerAddress,
      updatedAt: now
    });
  }

  if (metadata?.hasDownloadableMedia) {
    await enqueueFollowUpMediaRefresh({
      redisConnection: context.redisConnection,
      payload: {
        chainId: payload.chainId,
        contractAddress: payload.contractAddress,
        tokenId: payload.tokenId,
        forceDownload: payload.forceMetadata
      }
    });
  }

  const collectionTokenCounts = await countTokensForCollections({
    database: context.database,
    collections: [
      {
        chainId: collection.chainId,
        contractAddress: collection.contractAddress
      }
    ]
  });

  await upsertCollection(context.database, {
    chainId: collection.chainId,
    contractAddress: collection.contractAddress,
    standard: collection.standard,
    name: collection.name,
    symbol: collection.symbol,
    baseUri: collection.baseUri,
    contractUriRaw: collection.contractUriRaw,
    contractUriResolved: collection.contractUriResolved,
    creatorName: collection.creatorName,
    creatorAddress: collection.creatorAddress,
    contractOwnerAddress: collection.contractOwnerAddress,
    royaltyRecipientAddress: collection.royaltyRecipientAddress,
    royaltyBasisPoints: collection.royaltyBasisPoints,
    collectionMetadataPayload: collection.collectionMetadataPayload,
    collectionMetadataHash: collection.collectionMetadataHash,
    lastCollectionMetadataFetchAt: collection.lastCollectionMetadataFetchAt,
    lastCollectionMetadataError: collection.lastCollectionMetadataError,
    description: collection.description,
    externalUrl: collection.externalUrl,
    imageOriginalUrl: collection.imageOriginalUrl,
    bannerImageOriginalUrl: collection.bannerImageOriginalUrl,
    featuredImageOriginalUrl: collection.featuredImageOriginalUrl,
    animationOriginalUrl: collection.animationOriginalUrl,
    audioOriginalUrl: collection.audioOriginalUrl,
    interactiveOriginalUrl: collection.interactiveOriginalUrl,
    totalSupply: collection.totalSupply,
    indexedTokenCount:
      collectionTokenCounts.get(`${collection.chainId}:${collection.contractAddress}`) ?? collection.indexedTokenCount,
    deployBlock: collection.deployBlock,
    lastObservedBlock: collection.lastObservedBlock,
    lastIndexedBlock: collection.lastIndexedBlock,
    syncStatus: collection.syncStatus,
    lastSyncAt: now,
    createdAt: collection.createdAt,
    updatedAt: now
  });

  return {
    tokenUpdated: true,
    tokenCreated: !existingToken,
    metadataStatus: nextMetadataStatus,
    metadataVersion: nextMetadataVersion,
    ownerStateVersion: nextOwnerStateVersion,
    ownerAddress: onChainToken.ownerAddress,
    metadataUriResolved: onChainToken.metadataUriResolved,
    metadataError
  };
}

async function removeMissingTokenFromReadModel(params: {
  collection: CollectionDocument;
  existingToken: Awaited<ReturnType<typeof findTokenByIdentity>>;
  context: JobProcessingContext;
  updatedAt: Date;
}): Promise<void> {
  if (!params.existingToken) {
    return;
  }

  await deleteTokenAndDependents({
    database: params.context.database,
    chainId: params.existingToken.chainId,
    contractAddress: params.existingToken.contractAddress,
    tokenId: params.existingToken.tokenId
  });

  const collectionTokenCounts = await countTokensForCollections({
    database: params.context.database,
    collections: [
      {
        chainId: params.collection.chainId,
        contractAddress: params.collection.contractAddress
      }
    ]
  });

  await upsertCollection(params.context.database, {
    chainId: params.collection.chainId,
    contractAddress: params.collection.contractAddress,
    standard: params.collection.standard,
    name: params.collection.name,
    symbol: params.collection.symbol,
    baseUri: params.collection.baseUri,
    contractUriRaw: params.collection.contractUriRaw,
    contractUriResolved: params.collection.contractUriResolved,
    creatorName: params.collection.creatorName,
    creatorAddress: params.collection.creatorAddress,
    contractOwnerAddress: params.collection.contractOwnerAddress,
    royaltyRecipientAddress: params.collection.royaltyRecipientAddress,
    royaltyBasisPoints: params.collection.royaltyBasisPoints,
    collectionMetadataPayload: params.collection.collectionMetadataPayload,
    collectionMetadataHash: params.collection.collectionMetadataHash,
    lastCollectionMetadataFetchAt: params.collection.lastCollectionMetadataFetchAt,
    lastCollectionMetadataError: params.collection.lastCollectionMetadataError,
    description: params.collection.description,
    externalUrl: params.collection.externalUrl,
    imageOriginalUrl: params.collection.imageOriginalUrl,
    bannerImageOriginalUrl: params.collection.bannerImageOriginalUrl,
    featuredImageOriginalUrl: params.collection.featuredImageOriginalUrl,
    animationOriginalUrl: params.collection.animationOriginalUrl,
    audioOriginalUrl: params.collection.audioOriginalUrl,
    interactiveOriginalUrl: params.collection.interactiveOriginalUrl,
    totalSupply: params.collection.totalSupply,
    indexedTokenCount:
      collectionTokenCounts.get(`${params.collection.chainId}:${params.collection.contractAddress}`) ?? 0,
    deployBlock: params.collection.deployBlock,
    lastObservedBlock: params.collection.lastObservedBlock,
    lastIndexedBlock: params.collection.lastIndexedBlock,
    syncStatus: params.collection.syncStatus,
    lastSyncAt: params.updatedAt,
    createdAt: params.collection.createdAt,
    updatedAt: params.updatedAt
  });
}

async function materializeErc1155BalancesForToken(params: {
  chainId: number;
  contractAddress: string;
  tokenId: string;
  collection: CollectionDocument;
  context: JobProcessingContext;
  updatedAt: Date;
  publicClient: ReturnType<typeof getPublicClientForChain>;
}): Promise<{ holderCount: number; totalQuantity: string } | null> {
  if (params.collection.deployBlock === null) {
    return null;
  }

  const latestBlock = await params.publicClient
    .getBlockNumber()
    .then((value) => Number(value))
    .catch(() => null);

  if (latestBlock === null || latestBlock < params.collection.deployBlock) {
    return null;
  }

  const transfers = await readErc1155TransfersForTokenInRange({
    client: params.publicClient,
    contractAddress: params.contractAddress,
    tokenId: params.tokenId,
    fromBlock: params.collection.deployBlock,
    toBlock: latestBlock
  });
  const balancesByOwner = new Map<string, bigint>();

  for (const transfer of transfers) {
    applyErc1155TokenBalanceDelta({
      balancesByOwner,
      ownerAddress: transfer.fromAddress,
      delta: -BigInt(transfer.value)
    });
    applyErc1155TokenBalanceDelta({
      balancesByOwner,
      ownerAddress: transfer.toAddress,
      delta: BigInt(transfer.value)
    });
  }

  await replaceErc1155BalancesForToken(params.context.database, {
    chainId: params.chainId,
    contractAddress: params.contractAddress,
    tokenId: params.tokenId,
    balances: [...balancesByOwner.entries()].map(([ownerAddress, balance]) => ({
      chainId: params.chainId,
      contractAddress: params.contractAddress,
      tokenId: params.tokenId,
      ownerAddress,
      balance: balance.toString(),
      updatedAt: params.updatedAt
    }))
  });

  const totalQuantity = [...balancesByOwner.values()]
    .filter((balance) => balance > 0n)
    .reduce((total, balance) => total + balance, 0n);

  return {
    holderCount: [...balancesByOwner.values()].filter((balance) => balance > 0n).length,
    totalQuantity: totalQuantity.toString()
  };
}

async function detectErc1155TokenExistence(params: {
  payload: RefreshTokenJob;
  collection: CollectionDocument;
  context: JobProcessingContext;
  publicClient: ReturnType<typeof getPublicClientForChain>;
  onChainToken: {
    supplyQuantity: string | null;
  };
}): Promise<boolean | null> {
  const existingBalances = await listErc1155Balances({
    database: params.context.database,
    chainId: params.payload.chainId,
    contractAddress: params.payload.contractAddress,
    tokenId: params.payload.tokenId,
    limit: 1
  });

  if (existingBalances.length > 0) {
    return true;
  }

  if (params.onChainToken.supplyQuantity !== null && BigInt(params.onChainToken.supplyQuantity) > 0n) {
    return true;
  }

  if (params.collection.deployBlock === null) {
    return null;
  }

  const latestBlock = await params.publicClient
    .getBlockNumber()
    .then((value) => Number(value))
    .catch(() => null);

  if (latestBlock === null || latestBlock < params.collection.deployBlock) {
    return null;
  }

  const existsOnChain = await readErc1155TokenExists({
    client: params.publicClient,
    contractAddress: params.payload.contractAddress,
    tokenId: params.payload.tokenId
  });

  if (existsOnChain === true) {
    return true;
  }

  return hasErc1155TokenTransferActivity({
    client: params.publicClient,
    contractAddress: params.payload.contractAddress,
    tokenId: params.payload.tokenId,
    fromBlock: params.collection.deployBlock,
    toBlock: latestBlock
  });
}

function resolveNextMediaAssetId(params: {
  existingAssetId: ObjectId | null;
  existingSourceUrl: string | null;
  nextSourceUrl: string | null;
  shouldReconcile: boolean;
}): ObjectId | null {
  if (!params.shouldReconcile) {
    return params.existingAssetId;
  }

  if (params.existingSourceUrl === params.nextSourceUrl) {
    return params.existingAssetId;
  }

  return null;
}

async function handleRefreshMedia(
  payload: RefreshMediaJob,
  context: JobProcessingContext,
  job: Job
): Promise<Record<string, unknown>> {
  assertSupportedChain(payload.chainId);
  const now = new Date();
  const shouldScheduleRetry = hasRemainingAttempts(job);
  const token = await findTokenByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress,
    tokenId: payload.tokenId
  });

  if (!token) {
    throw new Error("Token does not exist for media refresh.");
  }

  const mediaSources = [
    token.imageOriginalUrl ? { kind: "image" as const, sourceUrl: token.imageOriginalUrl } : null,
    token.animationOriginalUrl
      ? { kind: "animation" as const, sourceUrl: token.animationOriginalUrl }
      : null,
    token.audioOriginalUrl ? { kind: "audio" as const, sourceUrl: token.audioOriginalUrl } : null
  ].filter((entry): entry is { kind: "image" | "animation" | "audio"; sourceUrl: string } =>
    Boolean(entry)
  );

  if (mediaSources.length === 0) {
    throw new Error("Token has no media sources to process.");
  }

  const storageClient = createStorageClient(context.storage);
  const assetIdsByKind: Partial<Record<"image" | "animation" | "audio", typeof token._id>> = {};
  const failedKinds: Array<"image" | "animation" | "audio"> = [];
  const retryPendingKinds: Array<"image" | "animation" | "audio"> = [];
  const externalFallbackKinds: Array<"image" | "animation" | "audio"> = [];
  let successfulUploads = 0;

  for (const mediaSource of mediaSources) {
    try {
      const normalizedSourceUrl = normalizeAssetUri(mediaSource.sourceUrl);
      const downloadedMedia = await loadMediaSource({
        sourceUrl: normalizedSourceUrl,
        kind: mediaSource.kind,
        mediaMaxVideoBytes: context.mediaMaxVideoBytes
      });
      const checksumSha256 = createHash("sha256").update(downloadedMedia.bytes).digest("hex");
      const objectKeys = buildMediaObjectKeys({
        chainId: token.chainId,
        contractAddress: token.contractAddress,
        tokenId: token.tokenId,
        kind: mediaSource.kind,
        checksumSha256
      });
      const uploadResult = await uploadStorageObject({
        client: storageClient,
        config: context.storage,
        key: objectKeys.original,
        body: downloadedMedia.bytes,
        contentType: downloadedMedia.mimeType,
        cacheControl: "public, max-age=31536000, immutable"
      });
      const imageDerivatives = await maybeBuildImageDerivatives({
        kind: mediaSource.kind,
        mimeType: downloadedMedia.mimeType,
        bytes: downloadedMedia.bytes
      });
      const optimizedUploadResult = imageDerivatives
        ? await uploadStorageObject({
            client: storageClient,
            config: context.storage,
            key: objectKeys.optimized,
            body: imageDerivatives.optimized.bytes,
            contentType: imageDerivatives.optimized.contentType,
            cacheControl: "public, max-age=31536000, immutable"
          })
        : null;
      const thumbnailUploadResult = imageDerivatives
        ? await uploadStorageObject({
            client: storageClient,
            config: context.storage,
            key: objectKeys.thumbnail,
            body: imageDerivatives.thumbnail.bytes,
            contentType: imageDerivatives.thumbnail.contentType,
            cacheControl: "public, max-age=31536000, immutable"
          })
        : null;
      const mediaAsset = await upsertMediaAsset(context.database, {
        tokenRef: token._id,
        kind: mediaSource.kind,
        sourceUrl: normalizedSourceUrl,
        storageKeyOriginal: objectKeys.original,
        storageKeyOptimized: optimizedUploadResult ? objectKeys.optimized : null,
        storageKeyThumbnail: thumbnailUploadResult ? objectKeys.thumbnail : null,
        cdnUrlOriginal: uploadResult.publicUrl,
        cdnUrlOptimized: optimizedUploadResult?.publicUrl ?? null,
        cdnUrlThumbnail: thumbnailUploadResult?.publicUrl ?? null,
        mimeType: downloadedMedia.mimeType,
        sizeBytes: downloadedMedia.sizeBytes,
        checksumSha256,
        width: imageDerivatives?.width ?? null,
        height: imageDerivatives?.height ?? null,
        durationSec: null,
        status: "ready",
        statusDetail: null,
        createdAt: now,
        updatedAt: new Date()
      });

      assetIdsByKind[mediaSource.kind] = mediaAsset._id;
      successfulUploads += 1;
    } catch (error) {
      const failure = classifyMediaProcessingFailure({
        kind: mediaSource.kind,
        error,
        mediaMaxVideoBytes: context.mediaMaxVideoBytes,
        sourceUrl: mediaSource.sourceUrl,
        shouldScheduleRetry
      });

      if (shouldKeepExternalMediaFallback({ kind: mediaSource.kind, error })) {
        externalFallbackKinds.push(mediaSource.kind);
      }

      if (failure.retryScheduled) {
        retryPendingKinds.push(mediaSource.kind);
      } else {
        failedKinds.push(mediaSource.kind);
      }

        const normalizedSourceUrl = normalizeAssetUri(mediaSource.sourceUrl);
        const fallbackChecksum = createHash("sha256").update(normalizedSourceUrl).digest("hex");
        const fallbackObjectKeys = buildMediaObjectKeys({
          chainId: token.chainId,
          contractAddress: token.contractAddress,
          tokenId: token.tokenId,
          kind: mediaSource.kind,
          checksumSha256: fallbackChecksum
        });

        await upsertMediaAsset(context.database, {
          tokenRef: token._id,
          kind: mediaSource.kind,
          sourceUrl: normalizedSourceUrl,
          storageKeyOriginal: fallbackObjectKeys.original,
          storageKeyOptimized: null,
          storageKeyThumbnail: null,
          cdnUrlOriginal: null,
          cdnUrlOptimized: null,
          cdnUrlThumbnail: null,
          mimeType: null,
          sizeBytes: null,
          checksumSha256: null,
          width: null,
          height: null,
          durationSec: null,
          status: failure.assetStatus,
          statusDetail: failure.statusDetail,
          createdAt: now,
          updatedAt: new Date()
        });
    }
  }

  const nextImageAssetId = assetIdsByKind.image ?? (failedKinds.includes("image") ? token.imageAssetId : token.imageAssetId);
  const nextAnimationAssetId =
    assetIdsByKind.animation ?? (failedKinds.includes("animation") ? token.animationAssetId : token.animationAssetId);
  const nextAudioAssetId = assetIdsByKind.audio ?? (failedKinds.includes("audio") ? token.audioAssetId : token.audioAssetId);
  const nextMediaStatus =
    retryPendingKinds.length > 0
      ? successfulUploads > 0 || externalFallbackKinds.length > 0
        ? "partial"
        : "processing"
      : failedKinds.length === 0
      ? successfulUploads > 0
        ? "ready"
        : "failed"
      : successfulUploads > 0 || externalFallbackKinds.length > 0
        ? "partial"
        : "failed";

  await upsertToken(context.database, {
    chainId: token.chainId,
    contractAddress: token.contractAddress,
    tokenId: token.tokenId,
    standard: token.standard,
    metadataUriRaw: token.metadataUriRaw,
    metadataUriResolved: token.metadataUriResolved,
    supplyQuantity: token.supplyQuantity,
    metadataStatus: token.metadataStatus,
    metadataVersion: token.metadataVersion,
    metadataHash: token.metadataHash,
    metadataPayload: token.metadataPayload,
    lastMetadataError: token.lastMetadataError,
    name: token.name,
    description: token.description,
    externalUrl: token.externalUrl,
    imageOriginalUrl: token.imageOriginalUrl,
    imageAssetId: nextImageAssetId,
    animationOriginalUrl: token.animationOriginalUrl,
    animationAssetId: nextAnimationAssetId,
    audioOriginalUrl: token.audioOriginalUrl,
    audioAssetId: nextAudioAssetId,
    interactiveOriginalUrl: token.interactiveOriginalUrl,
    interactiveMediaType: token.interactiveMediaType,
    attributes: token.attributes,
    mediaStatus: nextMediaStatus,
    ownerStateVersion: token.ownerStateVersion,
    lastMetadataFetchAt: token.lastMetadataFetchAt,
    lastMediaProcessAt: now,
    createdAt: token.createdAt,
    updatedAt: now
  });

  if (retryPendingKinds.length > 0) {
    throw new RetryableQueueError(
      `Temporary media fetch failure for ${retryPendingKinds.join(", ")}; retry scheduled while the source remains available.`
    );
  }

  return {
    mediaAssetsUploaded: successfulUploads,
    mediaAssetsFailed: failedKinds,
    mediaAssetsRetryPending: retryPendingKinds,
    mediaAssetsExternalFallback: externalFallbackKinds,
    mediaStatus: nextMediaStatus,
    forceDownload: payload.forceDownload
  };
}

function shouldKeepExternalMediaFallback(params: {
  kind: "image" | "animation" | "audio";
  error: unknown;
}): boolean {
  const message = params.error instanceof Error ? params.error.message : String(params.error ?? "");

  return params.kind === "animation" && message.includes("25 MB safety limit");
}

async function handleReindexRange(
  payload: ReindexRangeJob,
  context: JobProcessingContext
): Promise<Record<string, unknown>> {
  assertSupportedChain(payload.chainId);

  if (payload.fromBlock > payload.toBlock) {
    throw new Error("fromBlock must be less than or equal to toBlock.");
  }

  const now = new Date();
  const collection = await findCollectionByIdentity({
    database: context.database,
    chainId: payload.chainId,
    contractAddress: payload.contractAddress
  });

  if (!collection) {
    throw new Error("Collection is not registered. Run refresh/collection first.");
  }

  const ownerSyncResult =
    collection.standard === "erc1155"
      ? await materializeErc1155Balances({
          payload,
          chainId: collection.chainId,
          contractAddress: collection.contractAddress,
          deployBlock: collection.deployBlock,
          context,
          updatedAt: now
        })
      : collection.standard === "erc721"
        ? await materializeErc721Ownership({
            payload,
            chainId: collection.chainId,
            contractAddress: collection.contractAddress,
            deployBlock: collection.deployBlock,
            context,
            updatedAt: now
          })
        : null;
  const erc1155QuantitySyncResult =
    collection.standard === "erc1155"
      ? await refreshKnownErc1155TokenQuantities({
          collection,
          context,
          updatedAt: now
        })
      : null;

  await upsertCollection(context.database, {
    chainId: collection.chainId,
    contractAddress: collection.contractAddress,
    standard: collection.standard,
    name: collection.name,
    symbol: collection.symbol,
    baseUri: collection.baseUri,
    contractUriRaw: collection.contractUriRaw,
    contractUriResolved: collection.contractUriResolved,
    creatorName: collection.creatorName,
    creatorAddress: collection.creatorAddress,
    contractOwnerAddress: collection.contractOwnerAddress,
    royaltyRecipientAddress: collection.royaltyRecipientAddress,
    royaltyBasisPoints: collection.royaltyBasisPoints,
    collectionMetadataPayload: collection.collectionMetadataPayload,
    collectionMetadataHash: collection.collectionMetadataHash,
    lastCollectionMetadataFetchAt: collection.lastCollectionMetadataFetchAt,
    lastCollectionMetadataError: collection.lastCollectionMetadataError,
    description: collection.description,
    externalUrl: collection.externalUrl,
    imageOriginalUrl: collection.imageOriginalUrl,
    bannerImageOriginalUrl: collection.bannerImageOriginalUrl,
    featuredImageOriginalUrl: collection.featuredImageOriginalUrl,
    animationOriginalUrl: collection.animationOriginalUrl,
    audioOriginalUrl: collection.audioOriginalUrl,
    interactiveOriginalUrl: collection.interactiveOriginalUrl,
    totalSupply: collection.totalSupply,
    indexedTokenCount: collection.indexedTokenCount,
    deployBlock: collection.deployBlock,
    lastObservedBlock: Math.max(collection.lastObservedBlock ?? 0, payload.toBlock),
    lastIndexedBlock: Math.max(collection.lastIndexedBlock ?? 0, payload.toBlock),
    syncStatus: "active",
    lastSyncAt: now,
    createdAt: collection.createdAt,
    updatedAt: now
  });

  return {
    reindexed: true,
    fromBlock: payload.fromBlock,
    toBlock: payload.toBlock,
    ownerSync: ownerSyncResult,
    quantitySync: erc1155QuantitySyncResult
  };
}

async function refreshKnownErc1155TokenQuantities(params: {
  collection: CollectionDocument;
  context: JobProcessingContext;
  updatedAt: Date;
}): Promise<{
  processedTokenCount: number;
  positiveSupplyTokenCount: number;
  updatedTokenCount: number;
}> {
  if (params.collection.indexedTokenCount <= 0) {
    return {
      processedTokenCount: 0,
      positiveSupplyTokenCount: 0,
      updatedTokenCount: 0
    };
  }

  const publicClient = getPublicClientForChain(params.collection.chainId, params.context);
  const knownTokens = await listTokens({
    database: params.context.database,
    chainId: params.collection.chainId,
    contractAddress: params.collection.contractAddress,
    limit: params.collection.indexedTokenCount
  });
  let positiveSupplyTokenCount = 0;
  let updatedTokenCount = 0;

  for (const token of knownTokens) {
    const onChainToken = await readTokenOnChain({
      client: publicClient,
      contractAddress: params.collection.contractAddress,
      standard: "erc1155",
      tokenId: token.tokenId
    });
    const nextSupplyQuantity = onChainToken.supplyQuantity ?? token.supplyQuantity;

    if (nextSupplyQuantity !== null && BigInt(nextSupplyQuantity) > 0n) {
      positiveSupplyTokenCount += 1;
    }

    if (nextSupplyQuantity === token.supplyQuantity) {
      continue;
    }

    await upsertToken(params.context.database, {
      chainId: token.chainId,
      contractAddress: token.contractAddress,
      tokenId: token.tokenId,
      standard: token.standard,
      metadataUriRaw: token.metadataUriRaw,
      metadataUriResolved: token.metadataUriResolved,
      supplyQuantity: nextSupplyQuantity,
      metadataStatus: token.metadataStatus,
      metadataVersion: token.metadataVersion,
      metadataHash: token.metadataHash,
      metadataPayload: token.metadataPayload,
      lastMetadataError: token.lastMetadataError,
      name: token.name,
      description: token.description,
      externalUrl: token.externalUrl,
      imageOriginalUrl: token.imageOriginalUrl,
      imageAssetId: token.imageAssetId,
      animationOriginalUrl: token.animationOriginalUrl,
      animationAssetId: token.animationAssetId,
      audioOriginalUrl: token.audioOriginalUrl,
      audioAssetId: token.audioAssetId,
      interactiveOriginalUrl: token.interactiveOriginalUrl,
      interactiveMediaType: token.interactiveMediaType,
      attributes: token.attributes,
      mediaStatus: token.mediaStatus,
      ownerStateVersion: token.ownerStateVersion,
      lastMetadataFetchAt: token.lastMetadataFetchAt,
      lastMediaProcessAt: token.lastMediaProcessAt,
      createdAt: token.createdAt,
      updatedAt: params.updatedAt
    });
    updatedTokenCount += 1;
  }

  return {
    processedTokenCount: knownTokens.length,
    positiveSupplyTokenCount,
    updatedTokenCount
  };
}

async function materializeErc1155Balances(params: {
  payload: ReindexRangeJob;
  chainId: number;
  contractAddress: string;
  deployBlock: number | null;
  context: JobProcessingContext;
  updatedAt: Date;
}): Promise<{
  replayedFromBlock: number;
  snapshotBlock: number;
  holderCount: number;
  tokenCount: number;
  affectedTokens: string[];
}> {
  const publicClient = getPublicClientForChain(params.chainId, params.context);
  const replayedFromBlock = params.deployBlock ?? params.payload.fromBlock;
  const transfers = await readErc1155TransfersInRange({
    client: publicClient,
    contractAddress: params.contractAddress,
    fromBlock: replayedFromBlock,
    toBlock: params.payload.toBlock
  });
  const balancesByKey = new Map<string, { tokenId: string; ownerAddress: string; balance: bigint }>();
  const affectedTokens = new Set<string>();

  for (const transfer of transfers) {
    if (transfer.blockNumber >= params.payload.fromBlock) {
      affectedTokens.add(transfer.tokenId);
    }

    applyErc1155BalanceDelta({
      balancesByKey,
      tokenId: transfer.tokenId,
      ownerAddress: transfer.fromAddress,
      delta: -BigInt(transfer.value)
    });
    applyErc1155BalanceDelta({
      balancesByKey,
      tokenId: transfer.tokenId,
      ownerAddress: transfer.toAddress,
      delta: BigInt(transfer.value)
    });
  }

  await replaceErc1155BalancesForCollection(params.context.database, {
    chainId: params.chainId,
    contractAddress: params.contractAddress,
    balances: [...balancesByKey.values()]
      .filter((entry) => entry.balance > 0n)
      .map((entry) => ({
        chainId: params.chainId,
        contractAddress: params.contractAddress,
        tokenId: entry.tokenId,
        ownerAddress: entry.ownerAddress,
        balance: entry.balance.toString(),
        updatedAt: params.updatedAt
      }))
  });

  await Promise.all(
    [...affectedTokens].map((tokenId) =>
      bumpTokenOwnerStateVersion({
        database: params.context.database,
        chainId: params.chainId,
        contractAddress: params.contractAddress,
        tokenId,
        updatedAt: params.updatedAt
      })
    )
  );

  return {
    replayedFromBlock,
    snapshotBlock: params.payload.toBlock,
    holderCount: [...balancesByKey.values()].filter((entry) => entry.balance > 0n).length,
    tokenCount: new Set([...balancesByKey.values()].map((entry) => entry.tokenId)).size,
    affectedTokens: [...affectedTokens].sort((left, right) => Number(left) - Number(right))
  };
}

async function materializeErc721Ownership(params: {
  payload: ReindexRangeJob;
  chainId: number;
  contractAddress: string;
  deployBlock: number | null;
  context: JobProcessingContext;
  updatedAt: Date;
}): Promise<{
  replayedFromBlock: number;
  snapshotBlock: number;
  ownerCount: number;
  tokenCount: number;
  affectedTokens: string[];
}> {
  const publicClient = getPublicClientForChain(params.chainId, params.context);
  const replayedFromBlock = params.deployBlock ?? params.payload.fromBlock;
  const transfers = await readErc721TransfersInRange({
    client: publicClient,
    contractAddress: params.contractAddress,
    fromBlock: replayedFromBlock,
    toBlock: params.payload.toBlock
  });
  const ownershipsByToken = new Map<string, { tokenId: string; ownerAddress: string; updatedAt: Date }>();
  const affectedTokens = new Set<string>();

  for (const transfer of transfers) {
    if (transfer.blockNumber >= params.payload.fromBlock) {
      affectedTokens.add(transfer.tokenId);
    }

    if (transfer.toAddress) {
      ownershipsByToken.set(transfer.tokenId, {
        tokenId: transfer.tokenId,
        ownerAddress: transfer.toAddress,
        updatedAt: params.updatedAt
      });
      continue;
    }

    ownershipsByToken.delete(transfer.tokenId);
  }

  await replaceErc721OwnershipForCollection(params.context.database, {
    chainId: params.chainId,
    contractAddress: params.contractAddress,
    ownerships: [...ownershipsByToken.values()].map((ownership) => ({
      chainId: params.chainId,
      contractAddress: params.contractAddress,
      tokenId: ownership.tokenId,
      ownerAddress: ownership.ownerAddress,
      updatedAt: ownership.updatedAt
    }))
  });

  await Promise.all(
    [...affectedTokens].map((tokenId) =>
      bumpTokenOwnerStateVersion({
        database: params.context.database,
        chainId: params.chainId,
        contractAddress: params.contractAddress,
        tokenId,
        updatedAt: params.updatedAt
      })
    )
  );

  return {
    replayedFromBlock,
    snapshotBlock: params.payload.toBlock,
    ownerCount: new Set([...ownershipsByToken.values()].map((entry) => entry.ownerAddress)).size,
    tokenCount: ownershipsByToken.size,
    affectedTokens: [...affectedTokens].sort((left, right) => Number(left) - Number(right))
  };
}

function applyErc1155BalanceDelta(params: {
  balancesByKey: Map<string, { tokenId: string; ownerAddress: string; balance: bigint }>;
  tokenId: string;
  ownerAddress: string | null;
  delta: bigint;
}): void {
  if (!params.ownerAddress || params.delta === 0n) {
    return;
  }

  const key = `${params.tokenId}:${params.ownerAddress}`;
  const existing = params.balancesByKey.get(key);

  if (!existing) {
    params.balancesByKey.set(key, {
      tokenId: params.tokenId,
      ownerAddress: params.ownerAddress,
      balance: params.delta
    });
    return;
  }

  existing.balance += params.delta;

  if (existing.balance === 0n) {
    params.balancesByKey.delete(key);
  }
}

function applyErc1155TokenBalanceDelta(params: {
  balancesByOwner: Map<string, bigint>;
  ownerAddress: string | null;
  delta: bigint;
}): void {
  if (!params.ownerAddress || params.delta === 0n) {
    return;
  }

  const existing = params.balancesByOwner.get(params.ownerAddress) ?? 0n;
  const nextBalance = existing + params.delta;

  if (nextBalance <= 0n) {
    params.balancesByOwner.delete(params.ownerAddress);
    return;
  }

  params.balancesByOwner.set(params.ownerAddress, nextBalance);
}

async function bumpTokenOwnerStateVersion(params: {
  database: Db;
  chainId: number;
  contractAddress: string;
  tokenId: string;
  updatedAt: Date;
}): Promise<void> {
  const token = await findTokenByIdentity({
    database: params.database,
    chainId: params.chainId,
    contractAddress: params.contractAddress,
    tokenId: params.tokenId
  });

  if (!token) {
    return;
  }

  await upsertToken(params.database, {
    chainId: token.chainId,
    contractAddress: token.contractAddress,
    tokenId: token.tokenId,
    standard: token.standard,
    metadataUriRaw: token.metadataUriRaw,
    metadataUriResolved: token.metadataUriResolved,
    supplyQuantity: token.supplyQuantity,
    metadataStatus: token.metadataStatus,
    metadataVersion: token.metadataVersion,
    metadataHash: token.metadataHash,
    metadataPayload: token.metadataPayload,
    lastMetadataError: token.lastMetadataError,
    name: token.name,
    description: token.description,
    externalUrl: token.externalUrl,
    imageOriginalUrl: token.imageOriginalUrl,
    imageAssetId: token.imageAssetId,
    animationOriginalUrl: token.animationOriginalUrl,
    animationAssetId: token.animationAssetId,
    audioOriginalUrl: token.audioOriginalUrl,
    audioAssetId: token.audioAssetId,
    interactiveOriginalUrl: token.interactiveOriginalUrl,
    interactiveMediaType: token.interactiveMediaType,
    attributes: token.attributes,
    mediaStatus: token.mediaStatus,
    ownerStateVersion: token.ownerStateVersion + 1,
    lastMetadataFetchAt: token.lastMetadataFetchAt,
    lastMediaProcessAt: token.lastMediaProcessAt,
    createdAt: token.createdAt,
    updatedAt: params.updatedAt
  });
}

function assertSupportedChain(chainId: number): void {
  if (!isSupportedChainId(chainId)) {
    throw new Error(`Unsupported chainId ${chainId}.`);
  }
}

function getPublicClientForChain(chainId: number, context: JobProcessingContext) {
  return getChainPublicClient({
    chainId,
    rpcUrl: getRpcUrlForChain({
      chainId,
      rpcMainnetUrl: context.rpcMainnetUrl,
      rpcSepoliaUrl: context.rpcSepoliaUrl
    })
  });
}

type NormalizedMetadataMediaReference = {
  url: string;
  classification: "downloadable" | "interactive";
  interactiveMediaType: InteractiveMediaType | null;
};

async function resolveMetadata(
  metadataUri: {
    rawUri: string | null;
    resolvedUri: string | null;
  }
): Promise<{
  payload: Record<string, unknown>;
  payloadHash: string;
  name: string | null;
  description: string | null;
  externalUrl: string | null;
  imageOriginalUrl: string | null;
  animationOriginalUrl: string | null;
  audioOriginalUrl: string | null;
  interactiveOriginalUrl: string | null;
  interactiveMediaType: InteractiveMediaType | null;
  attributes: TokenAttribute[];
  hasMedia: boolean;
  hasDownloadableMedia: boolean;
} | null> {
  if (!metadataUri.rawUri && !metadataUri.resolvedUri) {
    return null;
  }

  const responseText = await loadMetadataPayloadText(buildMetadataFetchCandidateUrls(metadataUri));
  const parsedPayload = JSON.parse(responseText) as Record<string, unknown>;
  const imageReference =
    extractMetadataMediaReference(parsedPayload, [
      "image",
      "image_url",
      "imageUrl",
      "imageURL",
      "image_uri",
      "imageUri",
      "imageURI"
    ]) ??
    extractMetadataMediaReference(parsedPayload, ["image_data", "imageData"], {
      allowInlineSvg: true
    });
  const animationReference = extractMetadataMediaReference(parsedPayload, [
    "animation_url",
    "animationUrl",
    "animation",
    "video",
    "video_url",
    "videoUrl",
    "media",
    "media_url",
    "mediaUrl",
    "content",
    "content_url",
    "contentUrl"
  ]);
  const htmlReference = extractMetadataMediaReference(
    parsedPayload,
    ["html_url", "htmlUrl", "interactive_url", "interactiveUrl", "iframe_url", "iframeUrl"],
    { forceInteractiveType: "html" }
  );
  const youtubeReference = extractMetadataMediaReference(parsedPayload, ["youtube_url", "youtubeUrl", "youtube"], {
    forceInteractiveType: "youtube"
  });
  const audioOriginalUrl = extractMetadataMediaValue(parsedPayload, [
    "audio",
    "audio_url",
    "audioUrl",
    "sound",
    "sound_url",
    "soundUrl"
  ]);
  const imageOriginalUrl = imageReference?.url ?? null;
  const animationOriginalUrl = animationReference?.classification === "downloadable" ? animationReference.url : null;
  const interactiveReference =
    youtubeReference ??
    htmlReference ??
    (animationReference?.classification === "interactive" ? animationReference : null);
  const interactiveOriginalUrl = interactiveReference?.url ?? null;
  const interactiveMediaType = interactiveReference?.interactiveMediaType ?? null;
  const externalUrl =
    extractMetadataUrl(parsedPayload, ["external_url", "externalUrl", "home_url", "homeUrl", "website"]) ??
    interactiveOriginalUrl;

  return {
    payload: parsedPayload,
    payloadHash: createHash("sha256").update(responseText).digest("hex"),
    name: extractMetadataString(parsedPayload, ["name", "title"]),
    description: extractMetadataString(parsedPayload, ["description", "desc"]),
    externalUrl,
    imageOriginalUrl,
    animationOriginalUrl,
    audioOriginalUrl,
    interactiveOriginalUrl,
    interactiveMediaType,
    attributes: normalizeAttributes(parsedPayload.attributes ?? parsedPayload.traits ?? parsedPayload.properties),
    hasMedia: Boolean(imageOriginalUrl || animationOriginalUrl || audioOriginalUrl || interactiveOriginalUrl),
    hasDownloadableMedia: Boolean(imageOriginalUrl || animationOriginalUrl || audioOriginalUrl)
  };
}

async function resolveCollectionMetadata(
  metadataUri: {
    rawUri: string | null;
    resolvedUri: string | null;
  }
): Promise<{
  payload: Record<string, unknown>;
  payloadHash: string;
  name: string | null;
  creatorName: string | null;
  creatorAddress: string | null;
  royaltyRecipientAddress: string | null;
  royaltyBasisPoints: number | null;
  description: string | null;
  externalUrl: string | null;
  imageOriginalUrl: string | null;
  bannerImageOriginalUrl: string | null;
  featuredImageOriginalUrl: string | null;
  animationOriginalUrl: string | null;
  audioOriginalUrl: string | null;
  interactiveOriginalUrl: string | null;
} | null> {
  if (!metadataUri.rawUri && !metadataUri.resolvedUri) {
    return null;
  }

  const responseText = await loadMetadataPayloadText(buildMetadataFetchCandidateUrls(metadataUri));
  const parsedPayload = JSON.parse(responseText) as Record<string, unknown>;
  const nestedCollectionPayload = getNestedObjectPropertyCaseInsensitive(parsedPayload, "collection");
  const payloads = nestedCollectionPayload ? [parsedPayload, nestedCollectionPayload] : [parsedPayload];
  const animationReference = extractMetadataMediaReferenceFromPayloads(payloads, [
    "animation_url",
    "animationUrl",
    "animation",
    "video",
    "video_url",
    "videoUrl",
    "media",
    "media_url",
    "mediaUrl",
    "content",
    "content_url",
    "contentUrl"
  ]);
  const htmlReference = extractMetadataMediaReferenceFromPayloads(
    payloads,
    ["html_url", "htmlUrl", "interactive_url", "interactiveUrl", "iframe_url", "iframeUrl"],
    { forceInteractiveType: "html" }
  );
  const youtubeReference = extractMetadataMediaReferenceFromPayloads(payloads, ["youtube_url", "youtubeUrl", "youtube"], {
    forceInteractiveType: "youtube"
  });
  const interactiveReference =
    youtubeReference ??
    htmlReference ??
    (animationReference?.classification === "interactive" ? animationReference : null);

  return {
    payload: parsedPayload,
    payloadHash: createHash("sha256").update(responseText).digest("hex"),
    name: extractMetadataStringFromPayloads(payloads, ["name", "title", "collection_name", "collectionName"]),
    creatorName:
      extractMetadataStringFromPayloads(payloads, ["creator_name", "creatorName", "artist_name", "artistName"]) ??
      extractMetadataPersonNameFromPayloads(payloads, ["creator", "creators", "artist", "artists", "author", "publisher"]),
    creatorAddress:
      extractMetadataAddressFromPayloads(payloads, ["creator_address", "creatorAddress"]) ??
      extractMetadataAddressFromPayloads(payloads, ["creator", "creators", "artist", "artists", "author"]),
    royaltyRecipientAddress:
      extractMetadataAddressFromPayloads(payloads, [
        "fee_recipient",
        "feeRecipient",
        "seller_fee_recipient",
        "sellerFeeRecipient",
        "payout_address",
        "payoutAddress",
        "royalty_address",
        "royaltyAddress"
      ]) ?? null,
    royaltyBasisPoints:
      extractMetadataNumberFromPayloads(payloads, [
        "seller_fee_basis_points",
        "sellerFeeBasisPoints",
        "fee_basis_points",
        "feeBasisPoints",
        "royalty_bps",
        "royaltyBps"
      ]) ?? null,
    description: extractMetadataStringFromPayloads(payloads, ["description", "collection_description", "collectionDescription"]),
    externalUrl: extractMetadataUrlFromPayloads(payloads, [
      "external_link",
      "externalLink",
      "external_url",
      "externalUrl",
      "website",
      "home_url",
      "homeUrl"
    ]),
    imageOriginalUrl:
      extractMetadataMediaValueFromPayloads(payloads, [
        "image",
        "image_url",
        "imageUrl",
        "imageURI",
        "profile_image",
        "profileImage",
        "icon",
        "logo"
      ]) ?? null,
    bannerImageOriginalUrl:
      extractMetadataMediaValueFromPayloads(payloads, [
        "banner_image",
        "bannerImage",
        "cover_image",
        "coverImage",
        "header_image",
        "headerImage"
      ]) ?? null,
    featuredImageOriginalUrl:
      extractMetadataMediaValueFromPayloads(payloads, ["featured_image", "featuredImage", "featured_media", "featuredMedia"]) ??
      null,
    animationOriginalUrl: animationReference?.classification === "downloadable" ? animationReference.url : null,
    audioOriginalUrl:
      extractMetadataMediaValueFromPayloads(payloads, ["audio", "audio_url", "audioUrl", "sound", "sound_url", "soundUrl"]) ??
      null,
    interactiveOriginalUrl: interactiveReference?.url ?? null
  };
}

async function loadMetadataPayloadText(metadataUris: string[]): Promise<string> {
  if (metadataUris.length === 0) {
    throw new Error("No metadata URI candidates were available.");
  }

  let lastError: Error | null = null;

  for (const metadataUri of metadataUris) {
    try {
      if (metadataUri.startsWith("data:")) {
        const inlineMetadata = decodeDataUrlText(metadataUri);

        if (inlineMetadata.length > 1_000_000) {
          throw new Error("Metadata payload exceeds the 1 MB safety limit.");
        }

        return inlineMetadata;
      }

      await assertSafeRemoteUrl(metadataUri);
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10_000);

      try {
        const response = await fetch(metadataUri, {
          redirect: "follow",
          signal: controller.signal,
          headers: {
            accept: "application/json,text/plain;q=0.9,*/*;q=0.1"
          }
        });

        if (!response.ok) {
          throw new Error(`Metadata request failed with status ${response.status}.`);
        }

        const contentLengthHeader = response.headers.get("content-length");

        if (contentLengthHeader && Number(contentLengthHeader) > 1_000_000) {
          throw new Error("Metadata payload exceeds the 1 MB safety limit.");
        }

        const responseText = await response.text();

        if (responseText.length > 1_000_000) {
          throw new Error("Metadata payload exceeds the 1 MB safety limit.");
        }

        return responseText;
      } finally {
        clearTimeout(timeout);
      }
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }

  throw lastError ?? new Error("Metadata payload could not be fetched from any candidate URI.");
}

function buildMetadataFetchCandidateUrls(params: {
  rawUri: string | null;
  resolvedUri: string | null;
}): string[] {
  const candidates = [
    ...(params.resolvedUri ? [params.resolvedUri] : []),
    ...(params.rawUri ? expandMetadataFetchCandidatesFromRawUri(params.rawUri) : [])
  ].map((candidate) => candidate.trim()).filter(Boolean);

  return [...new Set(candidates)];
}

function expandMetadataFetchCandidatesFromRawUri(rawUri: string): string[] {
  if (rawUri.startsWith("data:")) {
    return [rawUri];
  }

  if (rawUri.startsWith("http://") || rawUri.startsWith("https://") || rawUri.startsWith("ar://")) {
    return [normalizeAssetUri(rawUri)];
  }

  if (!rawUri.startsWith("ipfs://")) {
    return [];
  }

  const remainder = rawUri.slice("ipfs://".length).replace(/^\/+/, "");

  if (!remainder) {
    return [];
  }

  if (remainder.startsWith("ipfs/")) {
    return [`https://dweb.link/${remainder}`, `https://ipfs.io/${remainder}`];
  }

  if (remainder.startsWith("ipns/")) {
    return [`https://dweb.link/${remainder}`, `https://ipfs.io/${remainder}`];
  }

  const [namespace = "", ...pathSegments] = remainder.split("/");
  const normalizedPath = pathSegments.join("/");
  const suffix = normalizedPath ? `/${normalizedPath}` : "";

  if (looksLikeIpfsCid(namespace)) {
    return [
      `https://dweb.link/ipfs/${namespace}${suffix}`,
      `https://ipfs.io/ipfs/${namespace}${suffix}`
    ];
  }

  return [
    `https://dweb.link/ipns/${namespace}${suffix}`,
    `https://ipfs.io/ipns/${namespace}${suffix}`,
    `https://dweb.link/ipfs/${namespace}${suffix}`,
    `https://ipfs.io/ipfs/${namespace}${suffix}`
  ];
}

function looksLikeIpfsCid(value: string): boolean {
  return /^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(value) || /^b[a-z2-7]{20,}$/i.test(value);
}

async function assertSafeRemoteUrl(candidate: string): Promise<void> {
  const url = new URL(candidate);

  if (!["http:", "https:"].includes(url.protocol)) {
    throw new Error(`Unsupported metadata URI protocol: ${url.protocol}`);
  }

  const host = normalizeIpLiteral(url.hostname);

  if (host === "localhost" || host.endsWith(".localhost")) {
    throw new Error("Metadata URI points to a blocked private or loopback host.");
  }

  const resolvedAddresses = await resolveRemoteHostAddresses(host);

  if (resolvedAddresses.length === 0 || resolvedAddresses.some(isBlockedRemoteAddress)) {
    throw new Error("Metadata URI points to a blocked private, loopback, or link-local host.");
  }
}

async function resolveRemoteHostAddresses(host: string): Promise<string[]> {
  if (isIP(host)) {
    return [host];
  }

  try {
    const resolvedAddresses = await lookup(host, {
      all: true,
      verbatim: true
    });

    return [...new Set(resolvedAddresses.map((entry) => normalizeIpLiteral(entry.address)))];
  } catch {
    throw new Error("Metadata URI host could not be resolved.");
  }
}

function isBlockedRemoteAddress(address: string): boolean {
  const normalizedAddress = normalizeIpLiteral(address);

  if (isIPv4MappedIpv6Address(normalizedAddress)) {
    return isBlockedRemoteAddress(normalizedAddress.slice("::ffff:".length));
  }

  const ipFamily = isIP(normalizedAddress);

  if (ipFamily === 4) {
    return isBlockedIpv4Address(normalizedAddress);
  }

  if (ipFamily === 6) {
    return isBlockedIpv6Address(normalizedAddress);
  }

  return true;
}

function isBlockedIpv4Address(address: string): boolean {
  const octets = address.split(".").map((segment) => Number.parseInt(segment, 10));

  if (octets.length !== 4 || octets.some((segment) => !Number.isInteger(segment) || segment < 0 || segment > 255)) {
    return true;
  }

  const firstOctet = octets[0] ?? -1;
  const secondOctet = octets[1] ?? -1;

  if (firstOctet === 0 || firstOctet === 10 || firstOctet === 127) {
    return true;
  }

  if (firstOctet === 169 && secondOctet === 254) {
    return true;
  }

  if (firstOctet === 172 && secondOctet >= 16 && secondOctet <= 31) {
    return true;
  }

  if (firstOctet === 192 && secondOctet === 168) {
    return true;
  }

  if (firstOctet === 100 && secondOctet >= 64 && secondOctet <= 127) {
    return true;
  }

  if (firstOctet === 198 && (secondOctet === 18 || secondOctet === 19)) {
    return true;
  }

  return firstOctet >= 224;
}

function isBlockedIpv6Address(address: string): boolean {
  if (address === "::" || address === "::1") {
    return true;
  }

  const [leadingHextet = ""] = address.split(":");
  const leadingValue = Number.parseInt(leadingHextet || "0", 16);

  if (!Number.isFinite(leadingValue)) {
    return true;
  }

  if ((leadingValue & 0xfe00) === 0xfc00) {
    return true;
  }

  if ((leadingValue & 0xffc0) === 0xfe80) {
    return true;
  }

  return (leadingValue & 0xff00) === 0xff00;
}

function isIPv4MappedIpv6Address(address: string): boolean {
  return address.startsWith("::ffff:");
}

function normalizeIpLiteral(value: string): string {
  return value.trim().replace(/^\[/, "").replace(/\]$/, "").replace(/%.*$/, "").toLowerCase();
}

function asNullableString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeMetadataUrl(value: unknown): string | null {
  const asString = extractMetadataStringValue(value);
  return asString && isSupportedMetadataUrlReference(asString) ? normalizeAssetUri(asString) : null;
}

function extractMetadataMediaValue(
  payload: Record<string, unknown>,
  aliases: string[],
  options?: {
    allowInlineSvg?: boolean;
    forceInteractiveType?: InteractiveMediaType;
  }
): string | null {
  return extractMetadataMediaReference(payload, aliases, options)?.url ?? null;
}

function extractMetadataMediaReference(
  payload: Record<string, unknown>,
  aliases: string[],
  options?: {
    allowInlineSvg?: boolean;
    forceInteractiveType?: InteractiveMediaType;
  }
): NormalizedMetadataMediaReference | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = normalizeMetadataMediaValue(candidate, options);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataUrl(payload: Record<string, unknown>, aliases: string[]): string | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = normalizeMetadataUrl(candidate);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataMediaValueFromPayloads(
  payloads: Record<string, unknown>[],
  aliases: string[],
  options?: {
    allowInlineSvg?: boolean;
    forceInteractiveType?: InteractiveMediaType;
  }
): string | null {
  for (const payload of payloads) {
    const normalized = extractMetadataMediaValue(payload, aliases, options);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataMediaReferenceFromPayloads(
  payloads: Record<string, unknown>[],
  aliases: string[],
  options?: {
    allowInlineSvg?: boolean;
    forceInteractiveType?: InteractiveMediaType;
  }
): NormalizedMetadataMediaReference | null {
  for (const payload of payloads) {
    const normalized = extractMetadataMediaReference(payload, aliases, options);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataUrlFromPayloads(payloads: Record<string, unknown>[], aliases: string[]): string | null {
  for (const payload of payloads) {
    const normalized = extractMetadataUrl(payload, aliases);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataStringFromPayloads(payloads: Record<string, unknown>[], aliases: string[]): string | null {
  for (const payload of payloads) {
    const normalized = extractMetadataString(payload, aliases);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataAddressFromPayloads(payloads: Record<string, unknown>[], aliases: string[]): string | null {
  for (const payload of payloads) {
    const normalized = extractMetadataAddress(payload, aliases);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataNumberFromPayloads(payloads: Record<string, unknown>[], aliases: string[]): number | null {
  for (const payload of payloads) {
    const normalized = extractMetadataNumber(payload, aliases);

    if (normalized !== null) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataPersonNameFromPayloads(payloads: Record<string, unknown>[], aliases: string[]): string | null {
  for (const payload of payloads) {
    const normalized = extractMetadataPersonName(payload, aliases);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataString(payload: Record<string, unknown>, aliases: string[]): string | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = extractMetadataStringValue(candidate);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataAddress(payload: Record<string, unknown>, aliases: string[]): string | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = extractMetadataAddressValue(candidate);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataNumber(payload: Record<string, unknown>, aliases: string[]): number | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = extractMetadataNumberValue(candidate);

    if (normalized !== null) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataPersonName(payload: Record<string, unknown>, aliases: string[]): string | null {
  for (const alias of aliases) {
    const candidate = getObjectPropertyCaseInsensitive(payload, alias);
    const normalized = extractMetadataPersonNameValue(candidate);

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function getObjectPropertyCaseInsensitive(payload: Record<string, unknown>, key: string): unknown {
  if (key in payload) {
    return payload[key];
  }

  const matchingKey = Object.keys(payload).find((candidateKey) => candidateKey.toLowerCase() === key.toLowerCase());
  return matchingKey ? payload[matchingKey] : undefined;
}

function getNestedObjectPropertyCaseInsensitive(payload: Record<string, unknown>, key: string): Record<string, unknown> | null {
  const value = getObjectPropertyCaseInsensitive(payload, key);

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  return value as Record<string, unknown>;
}

function extractMetadataStringValue(value: unknown): string | null {
  const asString = asNullableString(value);

  if (asString) {
    return asString;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  const candidateObject = value as Record<string, unknown>;

  for (const key of ["url", "src", "uri", "href", "gateway", "raw", "original", "value"]) {
    const nestedValue = getObjectPropertyCaseInsensitive(candidateObject, key);
    const nestedString = asNullableString(nestedValue);

    if (nestedString) {
      return nestedString;
    }
  }

  return null;
}

function extractMetadataAddressValue(value: unknown): string | null {
  const asString = asNullableString(value);

  if (asString && /^0x[a-fA-F0-9]{40}$/.test(asString)) {
    return normalizeWalletAddress(asString);
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const normalized = extractMetadataAddressValue(entry);

      if (normalized) {
        return normalized;
      }
    }

    return null;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  const candidateObject = value as Record<string, unknown>;

  for (const key of ["address", "wallet", "account", "recipient", "receiver", "owner"]) {
    const normalized = extractMetadataAddressValue(getObjectPropertyCaseInsensitive(candidateObject, key));

    if (normalized) {
      return normalized;
    }
  }

  return null;
}

function extractMetadataNumberValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return Math.trunc(value);
  }

  if (typeof value === "string" && /^\d+$/.test(value.trim())) {
    return Number.parseInt(value.trim(), 10);
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const normalized = extractMetadataNumberValue(entry);

      if (normalized !== null) {
        return normalized;
      }
    }
  }

  return null;
}

function extractMetadataPersonNameValue(value: unknown): string | null {
  const asString = asNullableString(value);

  if (asString && !/^0x[a-fA-F0-9]{40}$/.test(asString)) {
    return asString;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const normalized = extractMetadataPersonNameValue(entry);

      if (normalized) {
        return normalized;
      }
    }

    return null;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  const candidateObject = value as Record<string, unknown>;

  for (const key of ["name", "display_name", "displayName", "username", "title", "label"]) {
    const normalized = extractMetadataStringValue(getObjectPropertyCaseInsensitive(candidateObject, key));

    if (normalized && !/^0x[a-fA-F0-9]{40}$/.test(normalized)) {
      return normalized;
    }
  }

  return null;
}

function normalizeMetadataMediaValue(
  value: unknown,
  options?: {
    allowInlineSvg?: boolean;
    forceInteractiveType?: InteractiveMediaType;
  }
): NormalizedMetadataMediaReference | null {
  const asString = extractMetadataStringValue(value);

  if (!asString) {
    return null;
  }

  if (asString.startsWith("data:")) {
    const interactiveMediaType = inferInteractiveMediaType(asString) ?? options?.forceInteractiveType ?? null;

    return {
      url: asString,
      classification: interactiveMediaType ? "interactive" : "downloadable",
      interactiveMediaType
    };
  }

  if (options?.allowInlineSvg && looksLikeInlineSvgMarkup(asString)) {
    return {
      url: createInlineSvgDataUrl(asString),
      classification: "downloadable",
      interactiveMediaType: null
    };
  }

  if (!isSupportedMetadataUrlReference(asString)) {
    return null;
  }

  const normalizedUrl = normalizeAssetUri(asString);
  const interactiveMediaType = options?.forceInteractiveType ?? inferInteractiveMediaType(normalizedUrl);

  return {
    url: normalizedUrl,
    classification: interactiveMediaType ? "interactive" : "downloadable",
    interactiveMediaType
  };
}

function isSupportedMetadataUrlReference(value: string): boolean {
  return /^(https?:\/\/|ipfs:\/\/|ar:\/\/|data:)/i.test(value);
}

function looksLikeInlineSvgMarkup(value: string): boolean {
  return /^<svg[\s>]/i.test(value.trim());
}

function createInlineSvgDataUrl(svgMarkup: string): string {
  return `data:image/svg+xml;charset=utf-8,${encodeURIComponent(svgMarkup)}`;
}

function inferInteractiveMediaType(value: string): InteractiveMediaType | null {
  if (/^data:text\/html/i.test(value) || /\.(html?|xhtml)(?:[?#].*)?$/i.test(value)) {
    return "html";
  }

  try {
    const url = new URL(value);
    const hostname = url.hostname.toLowerCase();

    if (hostname === "youtu.be" || hostname.endsWith("youtube.com") || hostname.endsWith("youtube-nocookie.com")) {
      return "youtube";
    }

    if (hostname.endsWith("vimeo.com")) {
      return "interactive";
    }
  } catch {
    return null;
  }

  return null;
}

async function maybeBuildImageDerivatives(params: {
  kind: "image" | "animation" | "audio";
  mimeType: string | null;
  bytes: Uint8Array;
}) {
  if (params.kind !== "image") {
    return null;
  }

  if (!params.mimeType?.startsWith("image/")) {
    return null;
  }

  if (params.mimeType.split(";")[0]?.trim().toLowerCase() === "image/svg+xml") {
    return null;
  }

  try {
    return await buildImageDerivatives({
      originalBytes: params.bytes
    });
  } catch {
    return null;
  }
}

async function loadMediaSource(params: {
  sourceUrl: string;
  kind: "image" | "animation" | "audio";
  mediaMaxVideoBytes: number;
}): Promise<{
  bytes: Uint8Array;
  mimeType: string | null;
  sizeBytes: number;
}> {
  if (params.sourceUrl.startsWith("data:")) {
    return decodeDataUrlMedia(params.sourceUrl);
  }

  const remoteMedia = await downloadRemoteMedia({
    sourceUrl: params.sourceUrl,
    kind: params.kind,
    mediaMaxVideoBytes: params.mediaMaxVideoBytes
  });

  return {
    ...remoteMedia,
    mimeType: detectMediaMimeType(remoteMedia.bytes, remoteMedia.mimeType)
  };
}

async function downloadRemoteMedia(params: {
  sourceUrl: string;
  kind: "image" | "animation" | "audio";
  mediaMaxVideoBytes: number;
}): Promise<{
  bytes: Uint8Array;
  mimeType: string | null;
  sizeBytes: number;
}> {
  await assertSafeRemoteUrl(params.sourceUrl);
  const byteLimit = getMediaByteLimit({ kind: params.kind, mediaMaxVideoBytes: params.mediaMaxVideoBytes });
  const candidateUrls = buildMediaFetchCandidateUrls(params.sourceUrl);
  const failures: Array<{ message: string; retryable: boolean }> = [];

  for (const candidateUrl of candidateUrls) {
    try {
      return await fetchRemoteMediaCandidate({
        candidateUrl,
        byteLimit
      });
    } catch (error) {
      failures.push(
        classifyRemoteMediaFetchFailure({
          sourceUrl: params.sourceUrl,
          candidateUrl,
          error
        })
      );
    }
  }

  const retryableFailure = failures.find((failure) => failure.retryable);

  if (retryableFailure) {
    throw new RetryableQueueError(retryableFailure.message);
  }

  throw new Error(failures[0]?.message ?? "Media fetch failed.");
}

async function fetchRemoteMediaCandidate(params: {
  candidateUrl: string;
  byteLimit: number;
}): Promise<{
  bytes: Uint8Array;
  mimeType: string | null;
  sizeBytes: number;
}> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 60_000);

  try {
    const response = await fetch(params.candidateUrl, {
      redirect: "follow",
      signal: controller.signal,
      headers: {
        accept: "image/*,video/*,audio/*,application/octet-stream;q=0.9,*/*;q=0.1"
      }
    });

    if (!response.ok) {
      throw new Error(`Media request failed with status ${response.status}.`);
    }

    const contentLengthHeader = response.headers.get("content-length");

    if (contentLengthHeader && Number(contentLengthHeader) > params.byteLimit) {
      throw new Error(`Media payload exceeds the ${formatByteLimit(params.byteLimit)} safety limit.`);
    }

    const bytes = new Uint8Array(await response.arrayBuffer());

    if (bytes.byteLength > params.byteLimit) {
      throw new Error(`Media payload exceeds the ${formatByteLimit(params.byteLimit)} safety limit.`);
    }

    return {
      bytes,
      mimeType: asNullableString(response.headers.get("content-type")),
      sizeBytes: bytes.byteLength
    };
  } finally {
    clearTimeout(timeout);
  }
}

function getMediaByteLimit(params: {
  kind: "image" | "animation" | "audio";
  mediaMaxVideoBytes: number;
}): number {
  return params.kind === "animation" ? params.mediaMaxVideoBytes : 25_000_000;
}

function formatByteLimit(value: number): string {
  const megabytes = Math.round((value / 1_000_000) * 10) / 10;
  return `${megabytes} MB`;
}

function describeMediaProcessingFailure(params: {
  kind: "image" | "animation" | "audio";
  error: unknown;
  mediaMaxVideoBytes: number;
}): string {
  const message = params.error instanceof Error ? params.error.message : String(params.error ?? "Unknown media error.");

  if (params.kind === "animation" && message.includes("safety limit")) {
    return `External fallback retained because the video exceeds the configured ${formatByteLimit(params.mediaMaxVideoBytes)} ingest limit.`;
  }

  return message;
}

function classifyMediaProcessingFailure(params: {
  kind: "image" | "animation" | "audio";
  error: unknown;
  mediaMaxVideoBytes: number;
  sourceUrl: string;
  shouldScheduleRetry: boolean;
}): {
  assetStatus: "processing" | "failed";
  statusDetail: string;
  retryScheduled: boolean;
} {
  const message = describeMediaProcessingFailure({
    kind: params.kind,
    error: params.error,
    mediaMaxVideoBytes: params.mediaMaxVideoBytes
  });
  const retryable = isRetryableMediaFailure({
    error: params.error,
    sourceUrl: params.sourceUrl
  });

  if (retryable && params.shouldScheduleRetry) {
    return {
      assetStatus: "processing",
      statusDetail: `Temporary media fetch failure; retry scheduled. Last error: ${message}`,
      retryScheduled: true
    };
  }

  return {
    assetStatus: "failed",
    statusDetail: message,
    retryScheduled: false
  };
}

function isRetryableMediaFailure(params: { error: unknown; sourceUrl: string }): boolean {
  if (params.error instanceof RetryableQueueError) {
    return true;
  }

  const message = params.error instanceof Error ? params.error.message : String(params.error ?? "");
  const isIpfsLikeSource = isIpfsLikeMediaUrl(params.sourceUrl);
  const statusMatch = message.match(/status\s+(\d{3})/i);
  const statusCode = statusMatch ? Number(statusMatch[1]) : null;

  if (message.includes("safety limit") || message.includes("Unsupported") || message.includes("blocked private or loopback")) {
    return false;
  }

  if (statusCode !== null) {
    if ([408, 425, 429, 500, 502, 503, 504, 522, 524].includes(statusCode)) {
      return true;
    }

    if (isIpfsLikeSource && [404, 410].includes(statusCode)) {
      return true;
    }

    return false;
  }

  return /aborted|timeout|timed out|fetch failed|network|econnreset|socket|temporar/i.test(message);
}

function classifyRemoteMediaFetchFailure(params: {
  sourceUrl: string;
  candidateUrl: string;
  error: unknown;
}): { message: string; retryable: boolean } {
  const originalMessage = params.error instanceof Error ? params.error.message : String(params.error ?? "Unknown media fetch error.");
  const retryable = isRetryableMediaFailure({
    error: params.error,
    sourceUrl: params.sourceUrl
  });

  return {
    message: candidateUrlMatchesSource(params.sourceUrl, params.candidateUrl)
      ? originalMessage
      : `${originalMessage} Gateway tried: ${params.candidateUrl}`,
    retryable
  };
}

function buildMediaFetchCandidateUrls(sourceUrl: string): string[] {
  const candidates = [sourceUrl];

  try {
    const url = new URL(sourceUrl);
    const normalizedPath = url.pathname.replace(/\/+/g, "/");

    if (!normalizedPath.startsWith("/ipfs/") && !normalizedPath.startsWith("/ipns/")) {
      return candidates;
    }

    const candidateHosts = [url.host, "dweb.link", "ipfs.io", "cloudflare-ipfs.com"];

    for (const host of candidateHosts) {
      const candidate = new URL(sourceUrl);
      candidate.host = host;
      candidate.protocol = "https:";
      candidates.push(candidate.toString());
    }
  } catch {
    return candidates;
  }

  return [...new Set(candidates)];
}

function isIpfsLikeMediaUrl(value: string): boolean {
  try {
    const url = new URL(value);
    const pathname = url.pathname.replace(/\/+/g, "/");
    return pathname.startsWith("/ipfs/") || pathname.startsWith("/ipns/");
  } catch {
    return /^ipfs:\/\//i.test(value);
  }
}

function candidateUrlMatchesSource(sourceUrl: string, candidateUrl: string): boolean {
  return sourceUrl === candidateUrl;
}

function decodeDataUrlMedia(sourceUrl: string): {
  bytes: Uint8Array;
  mimeType: string | null;
  sizeBytes: number;
} {
  const match = sourceUrl.match(/^data:([^,]*?),(.*)$/s);

  if (!match) {
    throw new Error("Invalid data URL media payload.");
  }

  const metadataSection = match[1] ?? "";
  const dataSection = match[2] ?? "";
  const isBase64 = /;base64/i.test(metadataSection);
  const mimeType = asNullableString(metadataSection.replace(/;base64/gi, "").split(";")[0] ?? "");
  const buffer = isBase64
    ? Buffer.from(dataSection, "base64")
    : Buffer.from(decodeURIComponent(dataSection), "utf8");

  if (buffer.byteLength > 25_000_000) {
    throw new Error("Media payload exceeds the 25 MB safety limit.");
  }

  const bytes = new Uint8Array(buffer);

  return {
    bytes,
    mimeType: detectMediaMimeType(bytes, mimeType),
    sizeBytes: buffer.byteLength
  };
}

function decodeDataUrlText(sourceUrl: string): string {
  const match = sourceUrl.match(/^data:([^,]*?),(.*)$/s);

  if (!match) {
    throw new Error("Invalid data URL metadata payload.");
  }

  const metadataSection = match[1] ?? "";
  const dataSection = match[2] ?? "";
  const isBase64 = /;base64/i.test(metadataSection);
  const buffer = isBase64
    ? Buffer.from(dataSection, "base64")
    : Buffer.from(decodeURIComponent(dataSection), "utf8");

  return buffer.toString("utf8");
}

function detectMediaMimeType(bytes: Uint8Array, reportedMimeType: string | null): string | null {
  const normalizedMimeType = reportedMimeType?.split(";")[0]?.trim().toLowerCase() ?? null;

  if (normalizedMimeType && normalizedMimeType !== "text/plain" && normalizedMimeType !== "application/octet-stream") {
    return reportedMimeType;
  }

  if (looksLikeSvgContent(bytes)) {
    return "image/svg+xml";
  }

  return reportedMimeType;
}

function looksLikeSvgContent(bytes: Uint8Array): boolean {
  const prefix = new TextDecoder("utf-8", { fatal: false }).decode(bytes.slice(0, 512)).trimStart();
  return /^<svg[\s>]/i.test(prefix) || /^<\?xml[\s\S]*?<svg[\s>]/i.test(prefix);
}

function normalizeAttributes(value: unknown): TokenAttribute[] {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return Object.entries(value as Record<string, unknown>).flatMap(([traitType, traitValue]) => {
      if (
        typeof traitValue !== "string" &&
        typeof traitValue !== "number" &&
        typeof traitValue !== "boolean"
      ) {
        return [];
      }

      return [{
        trait_type: traitType,
        value: traitValue,
        display_type: undefined
      } satisfies TokenAttribute];
    });
  }

  if (!Array.isArray(value)) {
    return [];
  }

  const attributes: TokenAttribute[] = [];

  for (const entry of value) {
    if (!entry || typeof entry !== "object") {
      continue;
    }

    const raw = entry as Record<string, unknown>;
    attributes.push({
      trait_type: asNullableString(raw.trait_type) ?? undefined,
      value:
        typeof raw.value === "string" ||
        typeof raw.value === "number" ||
        typeof raw.value === "boolean"
          ? raw.value
          : undefined,
      display_type: asNullableString(raw.display_type) ?? undefined
    });
  }

  return attributes;
}

export async function enqueueFollowUpMediaRefresh(params: {
  redisConnection: IORedis;
  payload: RefreshMediaJob;
}): Promise<string> {
  const queueAddOptions = buildQueueAddOptions(queueNames.refreshMedia, params.payload);
  const queueJobId = queueAddOptions.jobId;
  const queue = new Queue(queueNames.refreshMedia, {
    connection: params.redisConnection
  });

  const existingJob = await queue.getJob(queueJobId);

  if (existingJob) {
    const state = await existingJob.getState();

    if (state === "failed" || (state === "completed" && params.payload.forceDownload)) {
      await existingJob.remove();
    } else {
      await queue.close();
      return queueJobId;
    }
  }

  await queue.add(queueNames.refreshMedia, params.payload, queueAddOptions);
  await queue.close();

  return queueJobId;
}