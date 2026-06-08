import type { NextRequest } from "next/server";
import { resolveRequestIp } from "@nft-platform/security";
import { logger } from "./logger";
import { getRedisClient } from "./redis";

const rateLimitWindowSeconds = 60;

export type PublicRateLimitResult = {
  allowed: boolean;
  limit: number;
  remaining: number;
  retryAfterSeconds: number;
  degraded: boolean;
};

export async function consumePublicRateLimit(params: {
  request: NextRequest;
  namespace: string;
  limitPerMinute: number;
}): Promise<PublicRateLimitResult> {
  if (params.limitPerMinute <= 0) {
    return {
      allowed: true,
      limit: 0,
      remaining: 0,
      retryAfterSeconds: 0,
      degraded: false
    };
  }

  const clientIp = resolveRequestIp(params.request.headers) ?? "unknown";
  const windowBucket = Math.floor(Date.now() / 1000 / rateLimitWindowSeconds);
  const redisKey = `public-rate-limit:${params.namespace}:${clientIp}:${windowBucket}`;

  try {
    const redis = getRedisClient();

    await redis.connect().catch(() => undefined);

    const countResult = await redis.eval(
      `local current = redis.call("INCR", KEYS[1])
if current == 1 then
  redis.call("EXPIRE", KEYS[1], ARGV[1])
end
return current`,
      1,
      redisKey,
      String(rateLimitWindowSeconds + 5)
    );

    const requestCount = typeof countResult === "number" ? countResult : Number(countResult);

    if (!Number.isFinite(requestCount)) {
      throw new Error("Public rate limiter backend returned a non-numeric counter value.");
    }

    return {
      allowed: requestCount <= params.limitPerMinute,
      limit: params.limitPerMinute,
      remaining: Math.max(params.limitPerMinute - requestCount, 0),
      retryAfterSeconds: requestCount <= params.limitPerMinute ? 0 : rateLimitWindowSeconds,
      degraded: false
    };
  } catch (error) {
    logger.warn("public_rate_limiter_backend_unavailable", {
      namespace: params.namespace,
      limitPerMinute: params.limitPerMinute,
      error
    });

    return {
      allowed: true,
      limit: params.limitPerMinute,
      remaining: params.limitPerMinute,
      retryAfterSeconds: 0,
      degraded: true
    };
  }
}

export function attachPublicRateLimitHeaders(response: Response, result: PublicRateLimitResult): void {
  response.headers.set("x-ratelimit-limit", String(result.limit));
  response.headers.set("x-ratelimit-remaining", String(result.remaining));
  response.headers.set("x-ratelimit-window-seconds", String(rateLimitWindowSeconds));

  if (result.retryAfterSeconds > 0) {
    response.headers.set("retry-after", String(result.retryAfterSeconds));
  }

  if (result.degraded) {
    response.headers.set("x-ratelimit-degraded", "1");
  }
}