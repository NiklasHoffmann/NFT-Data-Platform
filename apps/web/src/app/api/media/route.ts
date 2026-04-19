import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import type { NextRequest } from "next/server";
import { getWebRuntimeConfig } from "../../../lib/env";

export const dynamic = "force-dynamic";

const mediaProxyTimeoutMs = 15_000;

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

  const config = getWebRuntimeConfig();
  const storageObjectKey = resolveStorageObjectKey({
    targetUrl,
    mediaPublicBaseUrl: config.mediaPublicBaseUrl
  });

  if (storageObjectKey && hasConfiguredStorageCredentials(config)) {
    const storageResponse = await fetchStorageObject({
      endpoint: config.storageEndpoint,
      region: config.storageRegion,
      accessKey: config.storageAccessKey,
      secretKey: config.storageSecretKey,
      bucket: config.storageBucket,
      key: storageObjectKey,
      requestHeaders: buildUpstreamHeaders(request)
    });

    if (storageResponse) {
      return storageResponse;
    }
  }

  const upstream = await fetch(targetUrl, {
    method: "GET",
    headers: buildUpstreamHeaders(request),
    redirect: "follow",
    signal: AbortSignal.timeout(mediaProxyTimeoutMs)
  }).catch((error) => error);

  if (upstream instanceof Error) {
    if (upstream.name === "TimeoutError" || upstream.name === "AbortError") {
      return Response.json({ error: "Media proxy upstream timed out." }, { status: 504 });
    }

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

async function fetchStorageObject(params: {
  endpoint: string;
  region: string;
  accessKey: string;
  secretKey: string;
  bucket: string;
  key: string;
  requestHeaders: Headers;
}): Promise<Response | null> {
  const client = new S3Client({
    region: params.region,
    endpoint: params.endpoint,
    credentials: {
      accessKeyId: params.accessKey,
      secretAccessKey: params.secretKey
    },
    forcePathStyle: true
  });

  const range = params.requestHeaders.get("range") ?? undefined;
  const result = await client.send(
    new GetObjectCommand({
      Bucket: params.bucket,
      Key: params.key,
      ...(range ? { Range: range } : {})
    })
  ).catch((error) => error);

  if (result instanceof Error) {
    const statusCode = extractStorageErrorStatusCode(result);

    if (statusCode === 404) {
      return Response.json({ error: "Media asset not found in storage." }, { status: 404 });
    }

    if (statusCode === 416) {
      return Response.json({ error: "Requested media range is invalid." }, { status: 416 });
    }

    return null;
  }

  const body = typeof result.Body?.transformToWebStream === "function"
    ? result.Body.transformToWebStream()
    : null;

  if (!body) {
    return Response.json({ error: "Media asset body is unavailable." }, { status: 502 });
  }

  const responseHeaders = new Headers();

  for (const headerName of passthroughHeaders) {
    const value = resolveStorageHeaderValue(result, headerName);

    if (value) {
      responseHeaders.set(headerName, value);
    }
  }

  responseHeaders.set("x-media-proxy", "1");
  responseHeaders.set("x-content-type-options", "nosniff");

  return new Response(body, {
    status: result.$metadata.httpStatusCode ?? 200,
    headers: responseHeaders
  });
}

function resolveStorageObjectKey(params: {
  targetUrl: string;
  mediaPublicBaseUrl: string;
}): string | null {
  const target = new URL(params.targetUrl);
  const configuredMediaBase = new URL(params.mediaPublicBaseUrl);
  const normalizedMediaBasePath = configuredMediaBase.pathname.endsWith("/")
    ? configuredMediaBase.pathname
    : `${configuredMediaBase.pathname}/`;

  if (target.pathname === configuredMediaBase.pathname) {
    return null;
  }

  if (!target.pathname.startsWith(normalizedMediaBasePath)) {
    return null;
  }

  return decodeURIComponent(target.pathname.slice(normalizedMediaBasePath.length));
}

function hasConfiguredStorageCredentials(config: ReturnType<typeof getWebRuntimeConfig>): boolean {
  return Boolean(config.storageAccessKey.trim() && config.storageSecretKey.trim() && config.storageBucket.trim());
}

function extractStorageErrorStatusCode(error: Error & { $metadata?: { httpStatusCode?: number } }): number | null {
  return typeof error.$metadata?.httpStatusCode === "number" ? error.$metadata.httpStatusCode : null;
}

function resolveStorageHeaderValue(
  result: {
    ContentType?: string;
    ContentLength?: number;
    CacheControl?: string;
    ETag?: string;
    LastModified?: Date;
    AcceptRanges?: string;
    ContentRange?: string;
  },
  headerName: (typeof passthroughHeaders)[number]
): string | null {
  switch (headerName) {
    case "content-type":
      return result.ContentType ?? null;
    case "content-length":
      return typeof result.ContentLength === "number" ? String(result.ContentLength) : null;
    case "cache-control":
      return result.CacheControl ?? null;
    case "etag":
      return result.ETag ?? null;
    case "last-modified":
      return result.LastModified ? result.LastModified.toUTCString() : null;
    case "accept-ranges":
      return result.AcceptRanges ?? null;
    case "content-range":
      return result.ContentRange ?? null;
    default:
      return null;
  }
}