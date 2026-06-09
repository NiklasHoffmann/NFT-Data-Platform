import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import sharp from "sharp";
import { z } from "zod";

const ENDPOINT_UNREACHABLE_ERROR_CODES = new Set([
  "ENOTFOUND",
  "EAI_AGAIN",
  "ECONNREFUSED",
  "ECONNRESET",
  "ETIMEDOUT"
]);

export const storageConfigSchema = z.object({
  endpoint: z.string().url(),
  region: z.string().min(1),
  accessKey: z.string().min(1),
  secretKey: z.string().min(1),
  bucket: z.string().min(1),
  publicBaseUrl: z.string().url()
});

export type StorageConfig = z.infer<typeof storageConfigSchema>;

export function createStorageClient(config: StorageConfig): S3Client {
  const [primaryClient] = createStorageClientCandidates(config);

  if (!primaryClient) {
    throw new Error("No storage endpoint candidates configured.");
  }

  return primaryClient;
}

export function createStorageClientCandidates(config: StorageConfig): S3Client[] {
  const endpoints = buildStorageEndpointCandidates(config);

  return endpoints.map((endpoint) =>
    new S3Client({
      region: config.region,
      endpoint,
      credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey
      },
      forcePathStyle: true
    })
  );
}

export function isStorageEndpointUnreachableError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const nodeErrorCode = (error as { code?: unknown }).code;

  if (typeof nodeErrorCode === "string" && ENDPOINT_UNREACHABLE_ERROR_CODES.has(nodeErrorCode)) {
    return true;
  }

  const message = error.message.toLowerCase();

  return (
    message.includes("getaddrinfo enotfound") ||
    message.includes("eai_again") ||
    message.includes("econnrefused") ||
    message.includes("econnreset")
  );
}

function buildStorageEndpointCandidates(config: StorageConfig): string[] {
  const primaryEndpoint = normalizeEndpoint(config.endpoint);
  const candidates = [primaryEndpoint];

  const publicOrigin = normalizeEndpoint(new URL(config.publicBaseUrl).origin);

  if (!candidates.includes(publicOrigin)) {
    candidates.push(publicOrigin);
  }

  return candidates;
}

function normalizeEndpoint(endpoint: string): string {
  return endpoint.endsWith("/") ? endpoint.slice(0, -1) : endpoint;
}

export async function uploadStorageObject(params: {
  client: S3Client;
  config: StorageConfig;
  key: string;
  body: Uint8Array;
  contentType?: string | null;
  cacheControl?: string;
}): Promise<{ publicUrl: string }> {
  await params.client.send(
    new PutObjectCommand({
      Bucket: params.config.bucket,
      Key: params.key,
      Body: params.body,
      ContentType: params.contentType ?? undefined,
      CacheControl: params.cacheControl
    })
  );

  return {
    publicUrl: buildStorageObjectUrl(params.config, params.key)
  };
}

export function buildStorageObjectUrl(config: StorageConfig, key: string): string {
  const baseUrl = config.publicBaseUrl.endsWith("/")
    ? config.publicBaseUrl
    : `${config.publicBaseUrl}/`;

  return new URL(key, baseUrl).toString();
}

export async function buildImageDerivatives(params: {
  originalBytes: Uint8Array;
}): Promise<{
  width: number | null;
  height: number | null;
  optimized: {
    bytes: Uint8Array;
    contentType: string;
  };
  thumbnail: {
    bytes: Uint8Array;
    contentType: string;
  };
}> {
  const image = sharp(params.originalBytes, { failOn: "none" });
  const metadata = await image.metadata();

  const optimizedBuffer = await sharp(params.originalBytes, { failOn: "none" })
    .rotate()
    .resize({
      width: metadata.width && metadata.width > 1600 ? 1600 : undefined,
      withoutEnlargement: true
    })
    .webp({ quality: 82 })
    .toBuffer();

  const thumbnailBuffer = await sharp(params.originalBytes, { failOn: "none" })
    .rotate()
    .resize({
      width: 400,
      height: 400,
      fit: "inside",
      withoutEnlargement: true
    })
    .webp({ quality: 76 })
    .toBuffer();

  return {
    width: metadata.width ?? null,
    height: metadata.height ?? null,
    optimized: {
      bytes: new Uint8Array(optimizedBuffer),
      contentType: "image/webp"
    },
    thumbnail: {
      bytes: new Uint8Array(thumbnailBuffer),
      contentType: "image/webp"
    }
  };
}

export function buildMediaObjectKeys(params: {
  chainId: number;
  contractAddress: string;
  tokenId: string;
  kind: "image" | "video" | "audio" | "animation";
  checksumSha256: string;
}): {
  original: string;
  optimized: string;
  thumbnail: string;
} {
  const basePrefix = [
    params.chainId,
    params.contractAddress.toLowerCase(),
    params.tokenId,
    params.kind,
    params.checksumSha256
  ].join("/");

  return {
    original: `${basePrefix}/original`,
    optimized: `${basePrefix}/optimized`,
    thumbnail: `${basePrefix}/thumbnail`
  };
}
