import { ObjectId, type Db } from "mongodb";
import {
  countHoldersForCollections,
  countTokensForCollections,
  findLatestTokensForCollections,
  findRecentTokensForCollections,
  findMediaAssetsByIds,
  serializeCollectionDocument,
  serializeMediaAssetDocument,
  serializeTokenDocument,
  type CollectionDocument
} from "@nft-platform/db";

export async function serializeEnrichedCollection(database: Db, collection: CollectionDocument) {
  const [serializedCollection] = await serializeEnrichedCollections(database, [collection]);

  if (!serializedCollection) {
    throw new Error("Failed to serialize enriched collection response.");
  }

  return serializedCollection;
}

export async function serializeEnrichedCollections(database: Db, collections: CollectionDocument[]) {
  if (collections.length === 0) {
    return [];
  }

  const previewTokens = await findLatestTokensForCollections({
    database,
    collections: collections.map((collection) => ({
      chainId: collection.chainId,
      contractAddress: collection.contractAddress
    }))
  });
  const recentTokens = await findRecentTokensForCollections({
    database,
    collections: collections.map((collection) => ({
      chainId: collection.chainId,
      contractAddress: collection.contractAddress
    })),
    limitPerCollection: 6
  });
  const tokenCountsByCollection = await countTokensForCollections({
    database,
    collections: collections.map((collection) => ({
      chainId: collection.chainId,
      contractAddress: collection.contractAddress
    }))
  });
  const holderCountsByCollection = await countHoldersForCollections({
    database,
    collections: collections.map((collection) => ({
      chainId: collection.chainId,
      contractAddress: collection.contractAddress
    }))
  });
  const serializedPreviewTokens = previewTokens.map(serializeTokenDocument);
  const serializedRecentTokens = recentTokens.map(serializeTokenDocument);
  const previewTokensByCollection = new Map(
    serializedPreviewTokens.map((token) => [`${token.chainId}:${token.contractAddress}`, token])
  );
  const recentTokensByCollection = new Map<string, ReturnType<typeof serializeTokenDocument>[]>();

  for (const token of serializedRecentTokens) {
    const key = `${token.chainId}:${token.contractAddress}`;
    const collectionTokens = recentTokensByCollection.get(key) ?? [];
    collectionTokens.push(token);
    recentTokensByCollection.set(key, collectionTokens);
  }
  const mediaAssets = await findMediaAssetsByIds({
    database,
    assetIds: [...serializedPreviewTokens, ...serializedRecentTokens].flatMap((token) =>
      [token.imageAssetId, token.animationAssetId, token.audioAssetId]
        .filter((assetId): assetId is string => Boolean(assetId))
        .map((assetId) => new ObjectId(assetId))
    )
  });
  const mediaAssetsById = new Map(
    mediaAssets.map((mediaAsset) => [mediaAsset._id.toHexString(), serializeMediaAssetDocument(mediaAsset)])
  );

  return collections.map((collection) => {
    const previewToken = previewTokensByCollection.get(`${collection.chainId}:${collection.contractAddress}`);
    const serializedCollection = resolveSerializedCollectionMediaFields(serializeCollectionDocument(collection));
    const previewImage = previewToken?.imageAssetId ? mediaAssetsById.get(previewToken.imageAssetId) ?? null : null;
    const previewAnimation = previewToken?.animationAssetId ? mediaAssetsById.get(previewToken.animationAssetId) ?? null : null;
    const previewAudio = previewToken?.audioAssetId ? mediaAssetsById.get(previewToken.audioAssetId) ?? null : null;
    const recentCollectionTokens = recentTokensByCollection.get(`${collection.chainId}:${collection.contractAddress}`) ?? [];
    const hasRecentTokenImages = recentCollectionTokens.some((token) => {
      const imageAsset = token.imageAssetId ? mediaAssetsById.get(token.imageAssetId) ?? null : null;
      return Boolean(imageAsset?.cdnUrlThumbnail);
    });
    const coverImageSource = serializedCollection.imageOriginalUrl || serializedCollection.featuredImageOriginalUrl
      ? "collection-metadata"
      : hasRecentTokenImages
        ? "recent-tokens"
      : previewImage || previewAnimation || previewAudio
        ? "preview-token"
        : "none";

    return {
      ...serializedCollection,
      indexedTokenCount:
        tokenCountsByCollection.get(`${collection.chainId}:${collection.contractAddress}`) ??
        serializedCollection.indexedTokenCount,
      holderCount: holderCountsByCollection.get(`${collection.chainId}:${collection.contractAddress}`) ?? 0,
      coverImageSource,
      recentTokens: recentCollectionTokens.map((token) => ({
        tokenId: token.tokenId,
        name: token.name,
        mediaStatus: token.mediaStatus,
        updatedAt: token.updatedAt,
        image: token.imageAssetId ? mediaAssetsById.get(token.imageAssetId) ?? null : null
      })),
      preview: previewToken
        ? {
            tokenId: previewToken.tokenId,
            name: previewToken.name,
            mediaStatus: previewToken.mediaStatus,
            image: previewImage,
            animation: previewAnimation,
            audio: previewAudio
          }
        : null
    };
  });
}

function resolveSerializedCollectionMediaFields(collection: ReturnType<typeof serializeCollectionDocument>) {
  const payloads = getCollectionMetadataPayloadCandidates(collection.collectionMetadataPayload);
  const imageOriginalUrl =
    collection.imageOriginalUrl ??
    extractCollectionMediaValueFromPayloads(payloads, [
      "image",
      "image_url",
      "imageUrl",
      "imageURI",
      "profile_image",
      "profileImage",
      "icon",
      "logo"
    ]);
  const bannerImageOriginalUrl =
    collection.bannerImageOriginalUrl ??
    extractCollectionMediaValueFromPayloads(payloads, ["banner_image", "bannerImage", "cover_image", "coverImage", "header_image", "headerImage"]);
  const featuredImageOriginalUrl =
    collection.featuredImageOriginalUrl ??
    extractCollectionMediaValueFromPayloads(payloads, ["featured_image", "featuredImage", "featured_media", "featuredMedia"]);
  const animationCandidate = extractCollectionMediaValueFromPayloads(payloads, [
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
  const explicitInteractiveCandidate = extractCollectionMediaValueFromPayloads(payloads, [
    "html_url",
    "htmlUrl",
    "interactive_url",
    "interactiveUrl",
    "iframe_url",
    "iframeUrl",
    "youtube_url",
    "youtubeUrl",
    "youtube"
  ]);
  const interactiveOriginalUrl =
    collection.interactiveOriginalUrl ??
    explicitInteractiveCandidate ??
    (animationCandidate && isInteractiveCollectionMediaUrl(animationCandidate) ? animationCandidate : null);
  const animationOriginalUrl =
    collection.animationOriginalUrl ??
    (animationCandidate && !isInteractiveCollectionMediaUrl(animationCandidate) ? animationCandidate : null);
  const audioOriginalUrl =
    collection.audioOriginalUrl ??
    extractCollectionMediaValueFromPayloads(payloads, ["audio", "audio_url", "audioUrl", "sound", "sound_url", "soundUrl"]);

  return {
    ...collection,
    imageOriginalUrl,
    bannerImageOriginalUrl,
    featuredImageOriginalUrl,
    animationOriginalUrl,
    audioOriginalUrl,
    interactiveOriginalUrl
  };
}

function getCollectionMetadataPayloadCandidates(payload: Record<string, unknown> | null) {
  if (!payload) {
    return [] as Record<string, unknown>[];
  }

  const nestedCollectionPayload = getNestedObjectPropertyCaseInsensitive(payload, "collection");

  return nestedCollectionPayload ? [payload, nestedCollectionPayload] : [payload];
}

function extractCollectionMediaValueFromPayloads(payloads: Record<string, unknown>[], aliases: string[]) {
  for (const payload of payloads) {
    for (const alias of aliases) {
      const candidate = getObjectPropertyCaseInsensitive(payload, alias);
      const normalized = normalizeCollectionMediaValue(candidate);

      if (normalized) {
        return normalized;
      }
    }
  }

  return null;
}

function normalizeCollectionMediaValue(value: unknown): string | null {
  const asString = extractCollectionStringValue(value);

  if (!asString) {
    return null;
  }

  return /^(https?:\/\/|ipfs:\/\/|ar:\/\/|data:)/i.test(asString) ? asString : null;
}

function extractCollectionStringValue(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const candidateObject = value as Record<string, unknown>;

  for (const key of ["url", "src", "uri", "href", "gateway", "raw", "original", "value"]) {
    const nestedValue = getObjectPropertyCaseInsensitive(candidateObject, key);
    const nestedString = extractCollectionStringValue(nestedValue);

    if (nestedString) {
      return nestedString;
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

function isInteractiveCollectionMediaUrl(value: string): boolean {
  if (/^data:text\/html/i.test(value) || /\.(html?|xhtml)(?:[?#].*)?$/i.test(value)) {
    return true;
  }

  try {
    const url = new URL(value);
    const hostname = url.hostname.toLowerCase();
    return hostname === "youtu.be" || hostname.endsWith("youtube.com") || hostname.endsWith("youtube-nocookie.com");
  } catch {
    return false;
  }
}