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
  // The operator UI exposes indexed state, job payloads and write paths that queue
  // work, so it is gated. Everything under /api stays untouched: /api/v1 already
  // authenticates per request via HMAC, which is how consuming services such as the
  // marketplace reach it, and the health and media routes have to stay reachable for
  // monitoring and previews.
  if (!isApiRoute(request)) {
    const operatorUiDenial = denyOperatorUiRequest(request);

    if (operatorUiDenial) {
      return operatorUiDenial;
    }
  }

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

/**
 * Basic auth for the operator UI. Returns a response when the request must not pass,
 * null when it may. Missing credentials fail closed with 503 rather than leaving the
 * UI open: a deployment that forgot the variables is then loud instead of silently
 * unprotected.
 */
function denyOperatorUiRequest(request: NextRequest): NextResponse | null {
  const expectedUser = process.env.OPERATOR_UI_USER?.trim() ?? "";
  const expectedPassword = process.env.OPERATOR_UI_PASSWORD ?? "";

  if (!expectedUser || !expectedPassword) {
    return buildErrorResponse({
      status: 503,
      error: "operator_ui_not_configured",
      message:
        "The operator UI is unavailable until OPERATOR_UI_USER and OPERATOR_UI_PASSWORD are configured.",
      request
    });
  }

  const authorization = request.headers.get("authorization") ?? "";
  const credentials = decodeBasicCredentials(authorization);

  if (
    credentials &&
    matchesConstantTime(credentials.user, expectedUser) &&
    matchesConstantTime(credentials.password, expectedPassword)
  ) {
    return null;
  }

  // A browser that cancels the credential prompt renders this body, so visitors who
  // arrived looking for the public discover UI get an explanation and a way to make
  // contact instead of a raw error object. Scripted clients keep the JSON.
  const response = prefersHtmlResponse(request)
    ? new NextResponse(operatorUiUnauthorizedPage, {
        status: 401,
        headers: { "content-type": "text/html; charset=utf-8" }
      })
    : buildErrorResponse({
        status: 401,
        error: "operator_ui_unauthorized",
        message: "Authentication is required for the operator UI.",
        request
      });

  // Without this header the browser shows the body instead of prompting.
  response.headers.set("www-authenticate", 'Basic realm="NFT Data Platform", charset="UTF-8"');
  // Never let a proxy keep a 401 around for someone who has since been given access.
  response.headers.set("cache-control", "no-store");
  applyResponseSecurityHeaders(response, request);
  return response;
}

function prefersHtmlResponse(request: NextRequest): boolean {
  return (request.headers.get("accept") ?? "").includes("text/html");
}

/**
 * Shown to anyone who reaches the operator UI without credentials. Kept as a string
 * literal because middleware has no React runtime, and self-contained because the
 * stylesheet routes are behind this same gate.
 */
const operatorUiUnauthorizedPage = `<!doctype html>
<html lang="de">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex">
<title>Zugang beschränkt — NFT Data Platform</title>
<style>
  :root {
    color-scheme: light dark;
    --page: #f7f6f3;
    --surface: #ffffff;
    --ink: #1c1917;
    --ink-muted: #57534e;
    --accent: #7c2d12;
    --line: #e7e5e4;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --page: #1c1917;
      --surface: #262321;
      --ink: #f5f5f4;
      --ink-muted: #a8a29e;
      --accent: #fdba74;
      --line: #3f3b38;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    padding: 2rem 1.25rem;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    background: var(--page);
    color: var(--ink);
    font: 1rem/1.6 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  main {
    width: 100%;
    max-width: 34rem;
    background: var(--surface);
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    padding: 2rem 1.75rem;
  }
  h1 {
    margin: 0 0 1rem;
    font-size: clamp(1.5rem, 1.2rem + 1.5vw, 2rem);
    line-height: 1.2;
    letter-spacing: -0.01em;
  }
  p { margin: 0 0 1rem; max-width: 62ch; }
  p:last-child { margin-bottom: 0; }
  .muted { color: var(--ink-muted); }
  dl { margin: 1.5rem 0 0; padding-top: 1.25rem; border-top: 1px solid var(--line); }
  dt { font-size: 0.8125rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--ink-muted); }
  dd {
    margin: 0.35rem 0 0;
    font-size: 1.0625rem;
    font-weight: 600;
    overflow-wrap: anywhere;
  }
  a { color: var(--accent); }
  a:focus-visible { outline: 2px solid var(--accent); outline-offset: 3px; border-radius: 2px; }
</style>
</head>
<body>
<main>
  <h1>Diese Oberfläche ist nicht öffentlich</h1>
  <p>
    Sie sind auf der Betriebsoberfläche der NFT Data Platform gelandet. Sie war
    zeitweise offen erreichbar und ist inzwischen auf das Betriebsteam beschränkt.
    Das Durchsuchen von NFTs und Collections ist hier deshalb nicht mehr möglich.
  </p>
  <p>
    Wenn Sie NFT- und Collection-Daten verarbeiten wollen: die Plattform stellt
    dafür eine API bereit, deren Anfragen signiert werden. Zugangsdaten vergeben
    wir auf Anfrage.
  </p>
  <p class="muted">
    Schreiben Sie uns kurz, was Sie vorhaben — dann sagen wir Ihnen, ob die
    Plattform dazu passt.
  </p>
  <dl>
    <dt>Kontakt</dt>
    <dd>kontakt[a]pfalz-development.de</dd>
  </dl>
</main>
</body>
</html>`;

function decodeBasicCredentials(authorization: string): { user: string; password: string } | null {
  const [scheme, encoded] = authorization.split(" ");

  if (!scheme || scheme.toLowerCase() !== "basic" || !encoded) {
    return null;
  }

  try {
    const decoded = atob(encoded);
    const separatorIndex = decoded.indexOf(":");

    if (separatorIndex < 0) {
      return null;
    }

    return {
      user: decoded.slice(0, separatorIndex),
      password: decoded.slice(separatorIndex + 1)
    };
  } catch {
    return null;
  }
}

/**
 * Length-independent comparison, so a wrong password cannot be narrowed down by
 * timing. node:crypto is deliberately avoided here to keep the middleware free of
 * Node built-ins.
 */
function matchesConstantTime(candidate: string, expected: string): boolean {
  const candidateBytes = new TextEncoder().encode(candidate);
  const expectedBytes = new TextEncoder().encode(expected);
  let difference = candidateBytes.length ^ expectedBytes.length;

  for (let index = 0; index < Math.max(candidateBytes.length, expectedBytes.length); index += 1) {
    difference |= (candidateBytes[index] ?? 0) ^ (expectedBytes[index] ?? 0);
  }

  return difference === 0;
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
