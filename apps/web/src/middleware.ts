import { type NextRequest, NextResponse } from "next/server";

const defaultApiMaxRequestBytes = 1_048_576;
const apiWriteMethods = new Set(["POST", "PUT", "PATCH"]);
const allowedCorsOrigins = buildAllowedCorsOrigins();

/**
 * Reject stale or forged Next-Action requests before Next.js tries to
 * resolve them. Without this, an old browser bundle (or a scanner) that
 * sends an unknown action ID triggers an unhandledRejection in Next.js,
 * which makes the server appear unresponsive.
 */
export function middleware(request: NextRequest): NextResponse {
  if (request.headers.has("next-action")) {
    return buildErrorResponse({
      status: 400,
      error: "invalid_or_expired_action",
      message: "The submitted action is invalid or has expired.",
      request
    });
  }

  if (isApiRoute(request) && apiWriteMethods.has(request.method) && isRequestBodyTooLarge(request)) {
    return buildErrorResponse({
      status: 413,
      error: "request_too_large",
      message: "The request body exceeds the configured maximum size.",
      request
    });
  }

  if (isApiRoute(request) && hasCorsOrigin(request) && !isAllowedCorsOrigin(request)) {
    return buildErrorResponse({
      status: 403,
      error: "cors_origin_not_allowed",
      message: "The request origin is not allowed.",
      request
    });
  }

  if (isApiRoute(request) && request.method === "OPTIONS") {
    const preflightResponse = new NextResponse(null, { status: 204 });
    applyResponseSecurityHeaders(preflightResponse, request);
    applyCorsHeaders(preflightResponse, request);
    return preflightResponse;
  }

  const response = NextResponse.next();
  applyResponseSecurityHeaders(response, request);
  applyCorsHeaders(response, request);
  return response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"]
};

function isApiRoute(request: NextRequest): boolean {
  return request.nextUrl.pathname === "/api" || request.nextUrl.pathname.startsWith("/api/");
}

function isRequestBodyTooLarge(request: NextRequest): boolean {
  const contentLengthHeader = request.headers.get("content-length");

  if (!contentLengthHeader) {
    return false;
  }

  const contentLength = Number(contentLengthHeader);
  const maxRequestBytes = readApiMaxRequestBytes();

  return Number.isFinite(contentLength) && contentLength > maxRequestBytes;
}

function readApiMaxRequestBytes(): number {
  const parsed = Number(process.env.API_MAX_REQUEST_BYTES ?? defaultApiMaxRequestBytes);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    return defaultApiMaxRequestBytes;
  }

  return parsed;
}

function hasCorsOrigin(request: NextRequest): boolean {
  return Boolean(request.headers.get("origin"));
}

function isAllowedCorsOrigin(request: NextRequest): boolean {
  const origin = request.headers.get("origin");

  if (!origin) {
    return true;
  }

  return allowedCorsOrigins.has(origin);
}

function applyResponseSecurityHeaders(response: NextResponse, request: NextRequest): void {
  response.headers.set("x-content-type-options", "nosniff");
  response.headers.set("x-frame-options", "DENY");
  response.headers.set("referrer-policy", "no-referrer");
  response.headers.set("permissions-policy", "camera=(), microphone=(), geolocation=()");
  response.headers.set("cross-origin-opener-policy", "same-origin");
  response.headers.set("cross-origin-resource-policy", "same-origin");

  if (request.nextUrl.protocol === "https:") {
    response.headers.set("strict-transport-security", "max-age=31536000; includeSubDomains; preload");
  }
}

function applyCorsHeaders(response: NextResponse, request: NextRequest): void {
  if (!isApiRoute(request)) {
    return;
  }

  response.headers.set("vary", "Origin");
  response.headers.set("access-control-allow-methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
  response.headers.set("access-control-allow-headers", "content-type, x-client-id, x-api-key, x-signature, x-timestamp");
  response.headers.set("access-control-max-age", "600");

  const origin = request.headers.get("origin");

  if (origin && allowedCorsOrigins.has(origin)) {
    response.headers.set("access-control-allow-origin", origin);
  }
}

function buildAllowedCorsOrigins(): Set<string> {
  const configuredOrigins = [
    process.env.APP_BASE_URL ?? "",
    ...(process.env.CORS_ALLOWED_ORIGINS ?? "")
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean)
  ];

  return configuredOrigins.reduce<Set<string>>((origins, value) => {
    const normalizedOrigin = normalizeOrigin(value);

    if (normalizedOrigin) {
      origins.add(normalizedOrigin);
    }

    return origins;
  }, new Set());
}

function normalizeOrigin(value: string): string | null {
  try {
    return new URL(value).origin;
  } catch {
    return null;
  }
}

function buildErrorResponse(params: {
  status: number;
  error: string;
  message: string;
  request: NextRequest;
}): NextResponse {
  const response = NextResponse.json(
    {
      ok: false,
      error: params.error,
      message: params.message
    },
    {
      status: params.status
    }
  );

  applyResponseSecurityHeaders(response, params.request);
  applyCorsHeaders(response, params.request);
  return response;
}
