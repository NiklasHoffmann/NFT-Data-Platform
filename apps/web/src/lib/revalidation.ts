import type { TokenDocument } from "@nft-platform/db";
import { getWebRuntimeConfig } from "./env";
import { logger } from "./logger";
import { enqueueRefreshTokenJob } from "./queue";
import { getRedisClient } from "./redis";

export type TokenFreshness = {
  lastMetadataFetchAt: string | null;
  ageSeconds: number | null;
  isStale: boolean;
  revalidationQueued: boolean;
};

type StalenessInput = Pick<TokenDocument, "metadataStatus" | "lastMetadataFetchAt">;

/**
 * A token counts as stale once its metadata is older than the configured TTL. Tokens whose last
 * fetch failed use a separate, usually longer, window so that a permanently broken metadata URI
 * is retried occasionally instead of on every single read.
 */
export function isTokenMetadataStale(token: StalenessInput, now: Date): boolean {
  const config = getWebRuntimeConfig();

  if (!token.lastMetadataFetchAt) {
    return true;
  }

  const ttlSeconds =
    token.metadataStatus === "failed"
      ? config.tokenMetadataFailureRetrySeconds
      : config.tokenMetadataTtlSeconds;

  return resolveAgeSeconds(token.lastMetadataFetchAt, now) > ttlSeconds;
}

/**
 * Serves the already-materialized token and, when it has aged out, queues a refresh to run behind
 * the response. The read is never blocked on chain or metadata IO; the caller sees the current
 * state plus a `freshness` block telling it what it just got.
 */
export async function revalidateTokenIfStale(token: TokenDocument, now: Date): Promise<TokenFreshness> {
  const config = getWebRuntimeConfig();
  const isStale = isTokenMetadataStale(token, now);
  const freshness: TokenFreshness = {
    lastMetadataFetchAt: token.lastMetadataFetchAt ? token.lastMetadataFetchAt.toISOString() : null,
    ageSeconds: token.lastMetadataFetchAt ? resolveAgeSeconds(token.lastMetadataFetchAt, now) : null,
    isStale,
    revalidationQueued: false
  };

  if (!isStale || !config.readRevalidationEnabled) {
    return freshness;
  }

  const identity = `${token.chainId}:${token.contractAddress}:${token.tokenId}`;

  // Without this guard every read of the same stale token would queue its own refresh for as long
  // as the worker takes to catch up. The marker collapses that burst into one refresh per window.
  if (!(await claimRevalidationSlot(identity, config.readRevalidationDebounceSeconds))) {
    return freshness;
  }

  try {
    await enqueueRefreshTokenJob(
      {
        chainId: token.chainId,
        contractAddress: token.contractAddress,
        tokenId: token.tokenId,
        forceMetadata: false,
        forceOwnership: false
      },
      { allowReenqueueCompleted: true }
    );

    return {
      ...freshness,
      revalidationQueued: true
    };
  } catch (error) {
    logger.warn("token_revalidation_enqueue_failed", {
      chainId: token.chainId,
      contractAddress: token.contractAddress,
      tokenId: token.tokenId,
      error
    });

    return freshness;
  }
}

/**
 * Returns true for exactly one caller per identity and window. A Redis outage resolves to false so
 * that a degraded cache cannot turn every read into a queued job.
 */
async function claimRevalidationSlot(identity: string, windowSeconds: number): Promise<boolean> {
  try {
    const redis = getRedisClient();

    await redis.connect().catch(() => undefined);

    const claim = await redis.set(`revalidate:token:${identity}`, "1", "EX", windowSeconds, "NX");
    return claim === "OK";
  } catch (error) {
    logger.warn("token_revalidation_debounce_unavailable", {
      identity,
      error
    });

    return false;
  }
}

function resolveAgeSeconds(fetchedAt: Date, now: Date): number {
  return Math.max(0, Math.floor((now.getTime() - fetchedAt.getTime()) / 1000));
}
