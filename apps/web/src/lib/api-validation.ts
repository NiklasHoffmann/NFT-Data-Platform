import type { ZodError } from "zod";
import { decodeUpdatedAtCursor, type UpdatedAtCursor } from "./cursor-pagination";

export function buildValidationIssues(error: ZodError): Array<{ path: string; message: string }> {
  return error.issues.map((issue) => ({
    path: issue.path.join("."),
    message: issue.message
  }));
}

export function buildValidationErrorResponse(params: {
  error: string;
  issues: Array<{ path: string; message: string }>;
  status?: number;
  message?: string;
}): Response {
  return Response.json(
    {
      ok: false,
      error: params.error,
      ...(params.message ? { message: params.message } : {}),
      issues: params.issues
    },
    { status: params.status ?? 400 }
  );
}

export function safeParseJsonRequestBody(bodyText: string):
  | { ok: true; data: unknown }
  | { ok: false; response: Response } {
  if (!bodyText.trim()) {
    return { ok: true, data: {} };
  }

  try {
    return { ok: true, data: JSON.parse(bodyText) };
  } catch {
    return {
      ok: false,
      response: Response.json(
        {
          ok: false,
          error: "invalid_json_body",
          message: "The request body must contain valid JSON."
        },
        { status: 400 }
      )
    };
  }
}

export function safeDecodeUpdatedAtCursor(cursor: string):
  | { ok: true; value: UpdatedAtCursor }
  | { ok: false; response: Response } {
  try {
    return { ok: true, value: decodeUpdatedAtCursor(cursor) };
  } catch {
    return {
      ok: false,
      response: Response.json(
        {
          ok: false,
          error: "invalid_cursor",
          message: "The supplied cursor is invalid."
        },
        { status: 400 }
      )
    };
  }
}