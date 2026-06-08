import type { NextRequest } from "next/server";
import { buildApiErrorResponse } from "../../../../lib/api-response";
import { getWebRuntimeConfig } from "../../../../lib/env";
import { probeRedisHealth } from "../../../../lib/health-checks";
import {
  attachPublicRateLimitHeaders,
  consumePublicRateLimit
} from "../../../../lib/public-rate-limit";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest): Promise<Response> {
  const config = getWebRuntimeConfig();
  const rateLimit = await consumePublicRateLimit({
    request,
    namespace: "health:redis",
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

  const check = await probeRedisHealth();
  const isHealthy = check.status === "ok";

  const response = Response.json(
    {
      ok: isHealthy,
      service: "web",
      ...check
    },
    {
      status: isHealthy ? 200 : 503
    }
  );

  attachPublicRateLimitHeaders(response, rateLimit);
  return response;
}