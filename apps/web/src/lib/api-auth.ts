import type { NextRequest } from "next/server";
import { findApiClientByKeyHash, insertAuditLog, markApiClientUsed } from "@nft-platform/db";
import type { Scope } from "@nft-platform/domain";
import {
  buildBootstrapApiClient,
  canonicalizeSignedRequest,
  decryptSecret,
  extractAuthHeaders,
  hasRequiredScopes,
  isIpAllowed,
  isTimestampFresh,
  resolveRequestIp,
  sha256Hex,
  verifyRequestSignature,
  type AuthenticatedApiClient
} from "@nft-platform/security";
import { getWebRuntimeConfig } from "./env";
import { getWebMongoDatabase } from "./mongodb";
import { getRedisClient } from "./redis";

type AuthorizedRequest = {
  client: AuthenticatedApiClient;
  bodyText: string;
  ip: string | null;
  rateLimit: {
    limit: number;
    remaining: number;
  };
};

type AuthenticationFailure = {
  ok: false;
  response: Response;
  clientId: string;
  ip: string | null;
  rateLimitDecision: "allow" | "deny";
};

type AuthenticationSuccess = {
  ok: true;
  value: AuthorizedRequest;
};

type AuthenticationResult = AuthenticationFailure | AuthenticationSuccess;

export function withAuthenticatedRoute<TContext>(
  requiredScopes: Scope[],
  handler: (params: {
    request: NextRequest;
    context: TContext;
    auth: AuthorizedRequest;
  }) => Promise<Response> | Response
) {
  return async (request: NextRequest, context: TContext): Promise<Response> => {
    const startedAt = Date.now();
    const authResult = await authenticateApiRequest(request, requiredScopes);
    const requestedScope = requiredScopes[0] ?? null;

    if (!authResult.ok) {
      await writeAuditLog({
        clientId: authResult.clientId,
        scope: requestedScope,
        method: request.method,
        path: `${request.nextUrl.pathname}${request.nextUrl.search}`,
        statusCode: authResult.response.status,
        responseTimeMs: Date.now() - startedAt,
        ip: authResult.ip,
        rateLimitDecision: authResult.rateLimitDecision
      });

      return authResult.response;
    }

    try {
      const response = await handler({
        request,
        context,
        auth: authResult.value
      });

      attachRateLimitHeaders(response, authResult.value.rateLimit);

      await writeAuditLog({
        clientId: authResult.value.client.clientId,
        scope: requestedScope,
        method: request.method,
        path: `${request.nextUrl.pathname}${request.nextUrl.search}`,
        statusCode: response.status,
        responseTimeMs: Date.now() - startedAt,
        ip: authResult.value.ip,
        rateLimitDecision: "allow"
      });

      return response;
    } catch (error) {
      await writeAuditLog({
        clientId: authResult.value.client.clientId,
        scope: requestedScope,
        method: request.method,
        path: `${request.nextUrl.pathname}${request.nextUrl.search}`,
        statusCode: 500,
        responseTimeMs: Date.now() - startedAt,
        ip: authResult.value.ip,
        rateLimitDecision: "allow"
      });

      throw error;
    }
  };
}

export async function authenticateApiRequest(
  request: NextRequest,
  requiredScopes: Scope[]
): Promise<AuthenticationResult> {
  const config = getWebRuntimeConfig();
  const ip = resolveRequestIp(request.headers);
  const bodyText = request.method === "GET" || request.method === "HEAD" ? "" : await request.text();
  const headerValues = extractAuthHeaders(request.headers);
  const database = getWebMongoDatabase();

  if (!headerValues) {
    return buildFailureResponse({
      clientId: "unknown",
      ip,
      rateLimitDecision: "allow",
      status: 401,
      code: "missing_auth_headers",
      message: "Expected x-client-id, x-api-key, x-signature, and x-timestamp headers."
    });
  }

  const resolvedClient = await resolveApiClient({
    headerClientId: headerValues.clientId,
    apiKey: headerValues.apiKey,
    database
  });

  if (!resolvedClient) {
    return buildFailureResponse({
      clientId: headerValues.clientId,
      ip,
      rateLimitDecision: "allow",
      status: 401,
      code: "invalid_api_key",
      message: "The provided API credentials are not valid."
    });
  }

  if (!isIpAllowed(ip, resolvedClient.allowedIps)) {
    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "allow",
      status: 403,
      code: "ip_not_allowed",
      message: "The request IP is not allowlisted for this API client."
    });
  }

  if (
    !isTimestampFresh({
      timestamp: headerValues.timestamp,
      maxSkewSeconds: config.authMaxTimestampSkewSec
    })
  ) {
    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "allow",
      status: 401,
      code: "stale_timestamp",
      message: "The request timestamp is outside the accepted replay-protection window."
    });
  }

  const canonicalPayload = canonicalizeSignedRequest({
    method: request.method,
    path: `${request.nextUrl.pathname}${request.nextUrl.search}`,
    body: bodyText,
    timestamp: headerValues.timestamp
  });

  if (
    !verifyRequestSignature({
      payload: canonicalPayload,
      providedSignature: headerValues.signature,
      secret: resolvedClient.secret
    })
  ) {
    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "allow",
      status: 401,
      code: "invalid_signature",
      message: "The request signature could not be validated."
    });
  }

  if (!hasRequiredScopes(resolvedClient.scopes, requiredScopes)) {
    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "allow",
      status: 403,
      code: "missing_scope",
      message: "The API client does not have the required scope for this endpoint."
    });
  }

  try {
    const replayAccepted = await consumeReplayGuard({
      clientId: resolvedClient.clientId,
      signature: headerValues.signature,
      canonicalPayload,
      ttlSeconds: config.authMaxTimestampSkewSec + 5
    });

    if (!replayAccepted) {
      return buildFailureResponse({
        clientId: resolvedClient.clientId,
        ip,
        rateLimitDecision: "deny",
        status: 409,
        code: "replayed_request",
        message: "The signed request has already been used within the accepted replay-protection window."
      });
    }
  } catch (error) {
    console.error("[auth] replay guard backend unavailable", error);

    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "deny",
      status: 503,
      code: "replay_guard_backend_unavailable",
      message: "The authentication replay-protection backend is currently unavailable."
    });
  }

  let rateLimit: { allowed: boolean; remaining: number };

  try {
    rateLimit = await consumeRateLimit({
      clientId: resolvedClient.clientId,
      limit: resolvedClient.rateLimitPerMinute,
      windowSeconds: 60
    });
  } catch (error) {
    console.error("[auth] rate limit backend unavailable", error);

    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "deny",
      status: 503,
      code: "rate_limit_backend_unavailable",
      message: "The authentication rate-limit backend is currently unavailable."
    });
  }

  if (!rateLimit.allowed) {
    return buildFailureResponse({
      clientId: resolvedClient.clientId,
      ip,
      rateLimitDecision: "deny",
      status: 429,
      code: "rate_limit_exceeded",
      message: "The API client exceeded its configured rate limit."
    });
  }

  if (resolvedClient.source === "database") {
    void markApiClientUsed({
      database,
      clientId: resolvedClient.clientId,
      usedAt: new Date()
    }).catch((error) => {
      console.error("[auth] failed to mark api client usage", error);
    });
  }

  return {
    ok: true,
    value: {
      client: resolvedClient,
      bodyText,
      ip,
      rateLimit: {
        limit: resolvedClient.rateLimitPerMinute,
        remaining: rateLimit.remaining
      }
    }
  };
}

async function resolveApiClient(params: {
  headerClientId: string;
  apiKey: string;
  database: ReturnType<typeof getWebMongoDatabase>;
}): Promise<(AuthenticatedApiClient & { source: "database" | "env" }) | null> {
  const config = getWebRuntimeConfig();
  const keyHash = sha256Hex(params.apiKey);

  try {
    const databaseClient = await findApiClientByKeyHash({
      database: params.database,
      keyHash
    });

    if (databaseClient) {
      if (databaseClient.clientId !== params.headerClientId || databaseClient.status !== "active") {
        return null;
      }

      if (!databaseClient.secretEncrypted || !config.apiClientSecretEncryptionKey) {
        return null;
      }

      return {
        clientId: databaseClient.clientId,
        clientName: databaseClient.clientName,
        keyPrefix: databaseClient.keyPrefix,
        keyHash: databaseClient.keyHash,
        scopes: databaseClient.scopes,
        rateLimitPerMinute: databaseClient.rateLimitPerMinute,
        allowedIps: databaseClient.allowedIps,
        status: databaseClient.status,
        secret: decryptSecret({
          ciphertext: databaseClient.secretEncrypted,
          encryptionKey: config.apiClientSecretEncryptionKey
        }),
        source: "database"
      };
    }
  } catch (error) {
    console.error("[auth] failed to resolve api client from database", error);
  }

  if (!config.bootstrapClientId || !config.bootstrapApiKey || !config.bootstrapApiSecret) {
    return null;
  }

  const bootstrapClient = buildBootstrapApiClient({
    clientId: config.bootstrapClientId,
    apiKey: config.bootstrapApiKey,
    apiSecret: config.bootstrapApiSecret,
    scopes: config.bootstrapScopes,
    rateLimitPerMinute: config.bootstrapRateLimitPerMinute,
    allowedIps: config.bootstrapAllowedIps
  });

  if (
    params.headerClientId !== bootstrapClient.clientId ||
    keyHash !== bootstrapClient.keyHash
  ) {
    return null;
  }

  return {
    ...bootstrapClient,
    source: "env"
  };
}

async function consumeRateLimit(params: {
  clientId: string;
  limit: number;
  windowSeconds: number;
}): Promise<{ allowed: boolean; remaining: number }> {
  const bucket = Math.floor(Date.now() / 1000 / params.windowSeconds);
  const redisKey = `rate-limit:${params.clientId}:${bucket}`;
  const redis = getRedisClient();

  await redis.connect().catch(() => undefined);

  const count = await redis.incr(redisKey);

  if (count === 1) {
    await redis.expire(redisKey, params.windowSeconds + 5);
  }

  return {
    allowed: count <= params.limit,
    remaining: Math.max(params.limit - count, 0)
  };
}

async function consumeReplayGuard(params: {
  clientId: string;
  signature: string;
  canonicalPayload: string;
  ttlSeconds: number;
}): Promise<boolean> {
  const replayDigest = sha256Hex(`${params.signature}\n${params.canonicalPayload}`);
  const redisKey = `replay-guard:${params.clientId}:${replayDigest}`;
  const redis = getRedisClient();

  await redis.connect().catch(() => undefined);

  const writeResult = await redis.set(redisKey, "1", "EX", params.ttlSeconds, "NX");
  return writeResult === "OK";
}

async function writeAuditLog(params: {
  clientId: string;
  scope: Scope | null;
  method: string;
  path: string;
  statusCode: number;
  responseTimeMs: number;
  ip: string | null;
  rateLimitDecision: "allow" | "deny";
}): Promise<void> {
  try {
    await insertAuditLog(getWebMongoDatabase(), {
      clientId: params.clientId,
      scope: params.scope,
      method: params.method,
      path: params.path,
      statusCode: params.statusCode,
      responseTimeMs: params.responseTimeMs,
      ip: params.ip,
      timestamp: new Date(),
      rateLimitDecision: params.rateLimitDecision
    });
  } catch (error) {
    console.error("[audit] failed to persist audit log", error);
  }
}

function attachRateLimitHeaders(
  response: Response,
  rateLimit: { limit: number; remaining: number }
): void {
  response.headers.set("x-ratelimit-limit", String(rateLimit.limit));
  response.headers.set("x-ratelimit-remaining", String(rateLimit.remaining));
}

function buildFailureResponse(params: {
  clientId: string;
  ip: string | null;
  rateLimitDecision: "allow" | "deny";
  status: number;
  code: string;
  message: string;
}): AuthenticationFailure {
  return {
    ok: false,
    clientId: params.clientId,
    ip: params.ip,
    rateLimitDecision: params.rateLimitDecision,
    response: Response.json(
      {
        ok: false,
        error: params.code,
        message: params.message
      },
      {
        status: params.status
      }
    )
  };
}