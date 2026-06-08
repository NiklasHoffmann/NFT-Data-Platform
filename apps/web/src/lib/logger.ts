import process from "node:process";

type LogLevel = "debug" | "info" | "warn" | "error";

const sensitiveKeyPattern = /(secret|password|token|authorization|api[-_]?key|signature|cookie)/i;
const isDevelopment = process.env.NODE_ENV !== "production";

export const logger = {
  debug(event: string, context?: Record<string, unknown>): void {
    writeLog("debug", event, context);
  },
  info(event: string, context?: Record<string, unknown>): void {
    writeLog("info", event, context);
  },
  warn(event: string, context?: Record<string, unknown>): void {
    writeLog("warn", event, context);
  },
  error(event: string, context?: Record<string, unknown>): void {
    writeLog("error", event, context);
  }
};

function writeLog(level: LogLevel, event: string, context?: Record<string, unknown>): void {
  if (level === "debug" && !isDevelopment) {
    return;
  }

  const payload = {
    timestamp: new Date().toISOString(),
    level,
    event,
    ...(sanitizeValue(context ?? {}, 0) as Record<string, unknown>)
  };

  const serialized = JSON.stringify(payload);

  switch (level) {
    case "error":
      console.error(serialized);
      return;
    case "warn":
      console.warn(serialized);
      return;
    case "debug":
      console.debug(serialized);
      return;
    default:
      console.info(serialized);
  }
}

function sanitizeValue(value: unknown, depth: number): unknown {
  if (depth > 4) {
    return "[truncated]";
  }

  if (value instanceof Error) {
    return {
      name: value.name,
      message: value.message,
      ...(isDevelopment ? { stack: value.stack } : {})
    };
  }

  if (Array.isArray(value)) {
    return value.slice(0, 25).map((item) => sanitizeValue(item, depth + 1));
  }

  if (value && typeof value === "object") {
    const sanitized: Record<string, unknown> = {};

    for (const [key, nestedValue] of Object.entries(value as Record<string, unknown>)) {
      sanitized[key] = sensitiveKeyPattern.test(key)
        ? "[redacted]"
        : sanitizeValue(nestedValue, depth + 1);
    }

    return sanitized;
  }

  return value;
}