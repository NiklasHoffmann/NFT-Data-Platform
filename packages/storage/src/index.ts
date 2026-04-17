import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import sharp from "sharp";
import { z } from "zod";

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
  return new S3Client({
    region: config.region,
    endpoint: config.endpoint,
    credentials: {
      accessKeyId: config.accessKey,
      secretAccessKey: config.secretKey
    },
    forcePathStyle: true
  });
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
