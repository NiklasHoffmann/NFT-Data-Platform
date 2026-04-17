import {
  createCipheriv,
  createDecipheriv,
  createHash,
  createHmac,
  randomBytes,
  timingSafeEqual
} from "node:crypto";
import {
  apiClientStatusSchema,
  scopeSchema,
  type ApiClientStatus,
  type Scope
} from "@nft-platform/domain";

export const authHeaders = {
  clientId: "x-client-id",
  apiKey: "x-api-key",
  signature: "x-signature",
  timestamp: "x-timestamp"
} as const;

export type AuthHeaderValues = {
  clientId: string;
  apiKey: string;
  signature: string;
  timestamp: string;
};

export type AuthenticatedApiClient = {
  clientId: string;
  clientName: string;
  keyPrefix: string;
  keyHash: string;
  scopes: Scope[];
  rateLimitPerMinute: number;
  allowedIps: string[];
  status: ApiClientStatus;
  secret: string;
};

export function sha256Hex(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

export function buildApiKeyPrefix(apiKey: string): string {
  return apiKey.slice(0, Math.min(apiKey.length, 12));
}

export function canonicalizeSignedRequest(params: {
  method: string;
  path: string;
  body: string;
  timestamp: string;
}): string {
  const bodyHash = sha256Hex(params.body);
  return [params.method.toUpperCase(), params.path, bodyHash, params.timestamp].join("\n");
}

export function createRequestSignature(payload: string, secret: string): string {
  return createHmac("sha256", secret).update(payload).digest("hex");
}

export function verifyRequestSignature(params: {
  payload: string;
  providedSignature: string;
  secret: string;
}): boolean {
  if (!/^[a-fA-F0-9]{64}$/.test(params.providedSignature)) {
    return false;
  }

  const expectedSignature = createRequestSignature(params.payload, params.secret);
  const expectedBuffer = Buffer.from(expectedSignature, "hex");
  const providedBuffer = Buffer.from(params.providedSignature, "hex");

  if (expectedBuffer.length !== providedBuffer.length) {
    return false;
  }

  return timingSafeEqual(expectedBuffer, providedBuffer);
}

export function extractAuthHeaders(
  headers: Headers | Record<string, string | string[] | undefined>
): AuthHeaderValues | null {
  const clientId = getHeaderValue(headers, authHeaders.clientId);
  const apiKey = getHeaderValue(headers, authHeaders.apiKey);
  const signature = getHeaderValue(headers, authHeaders.signature);
  const timestamp = getHeaderValue(headers, authHeaders.timestamp);

  if (!clientId || !apiKey || !signature || !timestamp) {
    return null;
  }

  return {
    clientId,
    apiKey,
    signature,
    timestamp
  };
}

export function parseScopeList(value: string): Scope[] {
  if (!value.trim()) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => scopeSchema.parse(entry));
}

export function hasRequiredScopes(clientScopes: Scope[], requiredScopes: Scope[]): boolean {
  const grantedScopes = new Set(clientScopes);
  return requiredScopes.every((scope) => grantedScopes.has(scope));
}

export function isTimestampFresh(params: {
  timestamp: string;
  maxSkewSeconds: number;
  nowMs?: number;
}): boolean {
  const rawTimestamp = Number(params.timestamp);

  if (!Number.isFinite(rawTimestamp)) {
    return false;
  }

  const timestampMs = rawTimestamp > 10_000_000_000 ? rawTimestamp : rawTimestamp * 1000;
  const deltaMs = Math.abs((params.nowMs ?? Date.now()) - timestampMs);
  return deltaMs <= params.maxSkewSeconds * 1000;
}

export function resolveRequestIp(headers: Headers | Record<string, string | string[] | undefined>): string | null {
  const forwardedFor = getHeaderValue(headers, "x-forwarded-for");

  if (forwardedFor) {
    return forwardedFor.split(",")[0]?.trim() ?? null;
  }

  return getHeaderValue(headers, "x-real-ip") ?? null;
}

export function isIpAllowed(ip: string | null, allowedIps: string[]): boolean {
  if (allowedIps.length === 0) {
    return true;
  }

  if (!ip) {
    return false;
  }

  return allowedIps.includes(ip);
}

export function buildBootstrapApiClient(params: {
  clientId: string;
  clientName?: string;
  apiKey: string;
  apiSecret: string;
  scopes: Scope[];
  rateLimitPerMinute: number;
  allowedIps?: string[];
}): AuthenticatedApiClient {
  return {
    clientId: params.clientId,
    clientName: params.clientName ?? params.clientId,
    keyPrefix: buildApiKeyPrefix(params.apiKey),
    keyHash: sha256Hex(params.apiKey),
    scopes: params.scopes,
    rateLimitPerMinute: params.rateLimitPerMinute,
    allowedIps: params.allowedIps ?? [],
    status: apiClientStatusSchema.parse("active"),
    secret: params.apiSecret
  };
}

export function encryptSecret(params: { plaintext: string; encryptionKey: string }): string {
  const key = resolveEncryptionKey(params.encryptionKey);
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([cipher.update(params.plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();

  return [iv.toString("hex"), tag.toString("hex"), ciphertext.toString("hex")].join(":");
}

export function decryptSecret(params: { ciphertext: string; encryptionKey: string }): string {
  const [ivHex, tagHex, encryptedHex] = params.ciphertext.split(":");

  if (!ivHex || !tagHex || !encryptedHex) {
    throw new Error("Invalid encrypted secret payload format.");
  }

  const key = resolveEncryptionKey(params.encryptionKey);
  const decipher = createDecipheriv(
    "aes-256-gcm",
    key,
    Buffer.from(ivHex, "hex")
  );

  decipher.setAuthTag(Buffer.from(tagHex, "hex"));
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(encryptedHex, "hex")),
    decipher.final()
  ]);

  return plaintext.toString("utf8");
}

function getHeaderValue(
  headers: Headers | Record<string, string | string[] | undefined>,
  key: string
): string | null {
  if (headers instanceof Headers) {
    return headers.get(key);
  }

  const rawValue = headers[key] ?? headers[key.toLowerCase()] ?? headers[key.toUpperCase()];

  if (Array.isArray(rawValue)) {
    return rawValue[0] ?? null;
  }

  return rawValue ?? null;
}

function resolveEncryptionKey(input: string): Buffer {
  const trimmed = input.trim();

  if (/^[a-fA-F0-9]{64}$/.test(trimmed)) {
    return Buffer.from(trimmed, "hex");
  }

  const decoded = Buffer.from(trimmed, "base64");

  if (decoded.length !== 32) {
    throw new Error("Encryption key must be 32 bytes encoded as hex or base64.");
  }

  return decoded;
}
