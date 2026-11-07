import type { NextRequest } from "next/server";
import { buildApiErrorResponse } from "../../../lib/api-response";
import { getWebRuntimeConfig } from "../../../lib/env";
import {
  probeMongoHealth,
  probeRedisHealth,
  probeStorageHealth,
  readBackgroundHealth
} from "../../../lib/health-checks";
import {
  attachPublicRateLimitHeaders,
  consumePublicRateLimit
} from "../../../lib/public-rate-limit";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const config = getWebRuntimeConfig();
  const rateLimit = await consumePublicRateLimit({
    request,
    namespace: "health",
    limitPerMinute: config.publicReadRateLimitPerMinute
  });

  if (!rateLimit.allowed) {
    const rateLimitedResponse = buildApiErrorResponse({
      error: "rate_limit_exceeded",
      message: "Too many requests for health endpoints.",
      status: 429
    });

    attachPublicRateLimitHeaders(rateLimitedResponse, rateLimit);
    return rateLimitedResponse;
  }

  const [checks, background] = await Promise.all([
    Promise.all([probeMongoHealth(), probeRedisHealth(), probeStorageHealth()]),
    readBackgroundHealth()
  ]);
  const allHealthy = checks.every((check) => check.status === "ok");

  const response = Response.json(
    {
      ok: allHealthy,
      service: "web",
      status: allHealthy ? "ok" : "degraded",
      checks,
      background
    },
    {
      status: allHealthy ? 200 : 503
    }
  );

  attachPublicRateLimitHeaders(response, rateLimit);
  return response;
}
