import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import process from "node:process";

export type LoadLocalEnvFilesParams = {
  roots?: string[];
  includeProcessCwd?: boolean;
};

export function loadLocalEnvFiles(params: LoadLocalEnvFilesParams = {}): void {
  const roots: string[] = [];

  if (params.includeProcessCwd ?? true) {
    roots.push(process.cwd());
  }

  for (const root of params.roots ?? []) {
    roots.push(root);
  }

  const uniqueRoots = [...new Set(roots.map((root) => resolve(root)))];

  for (const root of uniqueRoots) {
    applyEnvFile(resolve(root, ".env.example"), false);
    applyEnvFile(resolve(root, ".env"), true);
  }
}

function applyEnvFile(filePath: string, override: boolean): void {
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