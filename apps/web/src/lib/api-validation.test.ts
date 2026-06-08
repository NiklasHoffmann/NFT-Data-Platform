import { describe, expect, it } from "vitest";
import { z } from "zod";
import {
  buildValidationErrorResponse,
  buildValidationIssues,
  safeDecodeUpdatedAtCursor,
  safeParseJsonRequestBody
} from "./api-validation";

describe("buildValidationIssues", () => {
  it("maps zod issues to flattened path/message pairs", () => {
    const schema = z.object({
      items: z.array(z.object({ tokenId: z.string().min(1) }))
    });
    const result = schema.safeParse({ items: [{ tokenId: "" }] });

    expect(result.success).toBe(false);

    if (result.success) {
      return;
    }

    expect(buildValidationIssues(result.error)).toEqual([
      {
        path: "items.0.tokenId",
        message: "String must contain at least 1 character(s)"
      }
    ]);
  });
});

describe("buildValidationErrorResponse", () => {
  it("builds a validation error response with explicit status", async () => {
    const response = buildValidationErrorResponse({
      error: "invalid_token_request",
      message: "Invalid request body.",
      status: 422,
      issues: [{ path: "items.0.tokenId", message: "Required" }]
    });

    expect(response.status).toBe(422);
    await expect(response.json()).resolves.toEqual({
      ok: false,
      error: "invalid_token_request",
      message: "Invalid request body.",
      issues: [{ path: "items.0.tokenId", message: "Required" }]
    });
  });
});

describe("safeParseJsonRequestBody", () => {
  it("parses valid json", () => {
    const result = safeParseJsonRequestBody('{"ok":true}');

    expect(result).toEqual({ ok: true, data: { ok: true } });
  });

  it("treats blank input as empty object", () => {
    const result = safeParseJsonRequestBody("   \n\t");

    expect(result).toEqual({ ok: true, data: {} });
  });

  it("returns a standardized error response for invalid json", async () => {
    const result = safeParseJsonRequestBody("{bad json}");

    expect(result.ok).toBe(false);

    if (result.ok) {
      return;
    }

    expect(result.response.status).toBe(400);
    await expect(result.response.json()).resolves.toEqual({
      ok: false,
      error: "invalid_json_body",
      message: "The request body must contain valid JSON."
    });
  });
});

describe("safeDecodeUpdatedAtCursor", () => {
  it("returns a standardized error response for invalid cursors", async () => {
    const result = safeDecodeUpdatedAtCursor("not-a-valid-cursor");

    expect(result.ok).toBe(false);

    if (result.ok) {
      return;
    }

    expect(result.response.status).toBe(400);
    await expect(result.response.json()).resolves.toEqual({
      ok: false,
      error: "invalid_cursor",
      message: "The supplied cursor is invalid."
    });
  });
});
