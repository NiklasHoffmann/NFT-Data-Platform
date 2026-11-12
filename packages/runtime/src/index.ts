import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";

export type LoadLocalEnvFilesParams = {
  roots?: string[];
  includeProcessCwd?: boolean;
};

/**
 * Loads the local baseline into `process.env`, in the order `.env.example` then `.env`, so a
 * missing local value still has the documented default behind it.
 *
 * What was already in the real environment when this runs always wins: a shell export, a Coolify
 * variable, a CI secret. Without that, a script pointed at a deployed instance through the
 * environment would be silently redirected back at the local stack by the `.env` sitting next to
 * it — the values are valid, the signature verifies, and the request reaches the wrong system.
 */
export function loadLocalEnvFiles(params: LoadLocalEnvFilesParams = {}): void {
  const environmentProvidedKeys = new Set(Object.keys(process.env));
  const roots: string[] = [];

  if (params.includeProcessCwd ?? true) {
    roots.push(process.cwd());
  }

  for (const root of params.roots ?? []) {
    roots.push(root);
  }

  const uniqueRoots = [...new Set(roots.map((root) => resolve(root)))];

  for (const root of uniqueRoots) {
    applyEnvFile(resolve(root, ".env.example"), false, environmentProvidedKeys);
    applyEnvFile(resolve(root, ".env"), true, environmentProvidedKeys);
  }
}

function applyEnvFile(filePath: string, override: boolean, environmentProvidedKeys: Set<string>): void {
  if (!existsSync(filePath)) {
    return;
  }

  const fileContent = readFileSync(filePath, "utf8");

  for (const rawLine of fileContent.split(/\r?\n/u)) {
    const line = rawLine.trim();

    if (!line || line.startsWith("#")) {
      continue;
    }

    const normalizedLine = line.startsWith("export ") ? line.slice(7) : line;
    const separatorIndex = normalizedLine.indexOf("=");

    if (separatorIndex <= 0) {
      continue;
    }

    const key = normalizedLine.slice(0, separatorIndex).trim();

    if (!key) {
      continue;
    }

    // `override` only decides whether a file beats a value an earlier file in this run put there.
    // It never reaches a value that came from the environment itself.
    if (environmentProvidedKeys.has(key)) {
      continue;
    }

    if (!override && process.env[key] !== undefined) {
      continue;
    }

    const value = normalizedLine.slice(separatorIndex + 1).trim();
    process.env[key] = stripWrappingQuotes(value);
  }
}

function stripWrappingQuotes(value: string): string {
  if (
    value.length >= 2 &&
    ((value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'")))
  ) {
    return value.slice(1, -1);
  }

  return value;
}