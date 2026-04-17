import type { Db } from "mongodb";
import { normalizeWalletAddress } from "@nft-platform/domain";
import {
  findMediaAssetsByIds,
  serializeMediaAssetDocument,
  serializeTokenDocument,
  type TokenDocument
} from "@nft-platform/db";

export async function serializeEnrichedToken(database: Db, token: TokenDocument) {
  const [serializedToken] = await serializeEnrichedTokens(database, [token]);

  if (!serializedToken) {
    throw new Error("Failed to serialize enriched token response.");
  }

  return serializedToken;
}

export async function serializeEnrichedTokens(database: Db, tokens: TokenDocument[]) {
  if (tokens.length === 0) {
    return [];
  }

  const mediaAssets = await findMediaAssetsByIds({
    database,
    assetIds: tokens.flatMap((token) =>
      [token.imageAssetId, token.animationAssetId, token.audioAssetId].filter(
        (assetId): assetId is NonNullable<typeof token.imageAssetId> => Boolean(assetId)
      )
    )
  });
  const mediaAssetsById = new Map(
    mediaAssets.map((mediaAsset) => [mediaAsset._id.toHexString(), serializeMediaAssetDocument(mediaAsset)])
  );

  return tokens.map((token) => {
    const serializedToken = serializeTokenDocument(token);
    const creator = deriveTokenCreator(serializedToken);

    return {
      ...serializedToken,
      creator,
      media: {
        image: serializedToken.imageAssetId ? mediaAssetsById.get(serializedToken.imageAssetId) ?? null : null,
        animation: serializedToken.animationAssetId
          ? mediaAssetsById.get(serializedToken.animationAssetId) ?? null
          : null,
        audio: serializedToken.audioAssetId ? mediaAssetsById.get(serializedToken.audioAssetId) ?? null : null
      }
    };
  });
}

function deriveTokenCreator(token: ReturnType<typeof serializeTokenDocument>) {
  return findCreatorInPayload(token.metadataPayload);
}

function findCreatorInPayload(payload: unknown): { name: string | null; address: string | null; source: string } | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  return walkCreatorSignal(payload as Record<string, unknown>, "metadata");
}

function walkCreatorSignal(
  value: unknown,
  sourcePath: string
): { name: string | null; address: string | null; source: string } | null {
  if (Array.isArray(value)) {
    for (const [index, entry] of value.entries()) {
      const match = walkCreatorSignal(entry, `${sourcePath}[${index}]`);

      if (match) {
        return match;
      }
    }

    return null;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  for (const [key, nested] of Object.entries(value)) {
    const normalizedKey = key.toLowerCase();
    const nextPath = `${sourcePath}.${key}`;

    if (/creator|artist|author|publisher|maker|brand|studio/i.test(normalizedKey)) {
      const name = extractCreatorName(nested);
      const address = extractCreatorAddress(nested);

      if (name || address) {
        return {
          name,
          address,
          source: nextPath
        };
      }
    }

    const nestedMatch = walkCreatorSignal(nested, nextPath);

    if (nestedMatch) {
      return nestedMatch;
    }
  }

  return null;
}

function extractCreatorName(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed && !isWalletAddress(trimmed) ? trimmed : null;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const match = extractCreatorName(entry);

      if (match) {
        return match;
      }
    }

    return null;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  const objectValue = value as Record<string, unknown>;

  for (const key of ["name", "display_name", "displayName", "title", "label", "username"]) {
    const candidate = objectValue[key];
    const match = extractCreatorName(candidate);

    if (match) {
      return match;
    }
  }

  return null;
}

function extractCreatorAddress(value: unknown): string | null {
  if (typeof value === "string") {
    return isWalletAddress(value) ? normalizeWalletAddress(value) : null;
  }

  if (Array.isArray(value)) {
    for (const entry of value) {
      const match = extractCreatorAddress(entry);

      if (match) {
        return match;
      }
    }

    return null;
  }

  if (!value || typeof value !== "object") {
    return null;
  }

  const objectValue = value as Record<string, unknown>;

  for (const key of ["address", "wallet", "account", "recipient", "receiver", "owner"]) {
    const candidate = objectValue[key];
    const match = extractCreatorAddress(candidate);

    if (match) {
      return match;
    }
  }

  return null;
}

function isWalletAddress(value: string): boolean {
  return /^0x[a-fA-F0-9]{40}$/.test(value.trim());
}