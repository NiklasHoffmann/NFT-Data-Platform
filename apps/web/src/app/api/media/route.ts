import type { NextRequest } from "next/server";
import { getWebRuntimeConfig } from "../../../lib/env";

export const dynamic = "force-dynamic";

const passthroughHeaders = [
  "content-type",
  "content-length",
  "cache-control",
  "etag",
  "last-modified",
  "accept-ranges",
  "content-range"
] as const;

export async function GET(request: NextRequest): Promise<Response> {
  const target = request.nextUrl.searchParams.get("url");

  if (!target) {
    return Response.json({ error: "Missing media url." }, { status: 400 });
  }

  const targetUrl = safeParseUrl(target);

  if (!targetUrl || !isAllowedMediaProxyTarget(targetUrl)) {
    return Response.json({ error: "Unsupported media url." }, { status: 400 });
  }

  const upstream = await fetch(targetUrl, {
    method: "GET",
    headers: buildUpstreamHeaders(request),
    redirect: "follow"
  }).catch(() => null);

  if (!upstream) {
    return Response.json({ error: "Media proxy request failed." }, { status: 502 });
  }

  const responseHeaders = new Headers();

  for (const headerName of passthroughHeaders) {
    const value = upstream.headers.get(headerName);

    if (value) {
      responseHeaders.set(headerName, value);
    }
  }

  responseHeaders.set("x-media-proxy", "1");
  responseHeaders.set("x-content-type-options", "nosniff");

  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: responseHeaders
  });
}

function buildUpstreamHeaders(request: NextRequest): Headers {
  const headers = new Headers();
  const accept = request.headers.get("accept");
  const range = request.headers.get("range");

  if (accept) {
    headers.set("accept", accept);
  }

  if (range) {
    headers.set("range", range);
  }

  return headers;
}

function safeParseUrl(value: string): string | null {
  try {
    const url = new URL(value);

    if (!["http:", "https:"].includes(url.protocol)) {
      return null;
    }

    return url.toString();
  } catch {
    return null;
  }
}

function isAllowedMediaProxyTarget(target: string): boolean {
  const config = getWebRuntimeConfig();
  const targetUrl = new URL(target);
  const configuredMediaBase = new URL(config.mediaPublicBaseUrl);
  const normalizedMediaBasePath = configuredMediaBase.pathname.endsWith("/")
    ? configuredMediaBase.pathname
    : `${configuredMediaBase.pathname}/`;

  if (
    targetUrl.origin === configuredMediaBase.origin &&
    (targetUrl.pathname === configuredMediaBase.pathname || targetUrl.pathname.startsWith(normalizedMediaBasePath))
  ) {
    return true;
  }

  return false;
}