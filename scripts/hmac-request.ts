import { readFileSync } from "node:fs";
import process from "node:process";
import { createHash, createHmac } from "node:crypto";
import { loadLocalEnvFiles } from "@nft-platform/runtime";

loadLocalEnvFiles();

async function main(): Promise<void> {
  const [methodArg, urlArg, bodyArg] = process.argv.slice(2);

  if (!methodArg || !urlArg) {
    throw new Error(
      "Usage: npm run api:request -- <METHOD> <URL> [JSON_BODY]"
    );
  }

  const method = methodArg.toUpperCase();
  const url = new URL(urlArg);
  const body = normalizeBody(bodyArg);
  const timestamp = Math.floor(Date.now() / 1000).toString();
  const path = `${url.pathname}${url.search}`;
  const payload = [
    method,
    path,
    createHash("sha256").update(body).digest("hex"),
    timestamp
  ].join("\n");

  const response = await fetch(url, {
    method,
    headers: {
      "content-type": "application/json",
      "x-client-id": requiredEnv("API_BOOTSTRAP_CLIENT_ID"),
      "x-api-key": requiredEnv("API_BOOTSTRAP_KEY"),
      "x-signature": createHmac("sha256", requiredEnv("API_BOOTSTRAP_SECRET"))
        .update(payload)
        .digest("hex"),
      "x-timestamp": timestamp
    },
    body: method === "GET" || method === "HEAD" ? undefined : body
  });

  const responseText = await response.text();
  console.log(`STATUS ${response.status}`);
  console.log(responseText);

  if (!response.ok) {
    process.exit(1);
  }
}

function normalizeBody(rawBody: string | undefined): string {
  if (!rawBody) {
    return "";
  }

  if (rawBody.startsWith("@")) {
    const filePath = rawBody.slice(1);
    const fileBody = readFileSync(filePath, "utf8").trim();
    JSON.parse(fileBody);
    return fileBody;
  }

  JSON.parse(rawBody);
  return rawBody;
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();

  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value;
}

main().catch((error) => {
  console.error("[api:request] failed", error);
  process.exit(1);
});