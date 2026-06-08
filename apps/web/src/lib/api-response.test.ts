import { describe, expect, it } from "vitest";
import { buildApiErrorResponse, buildApiSuccessResponse } from "./api-response";

describe("buildApiSuccessResponse", () => {
  it("returns an ok=true JSON payload and preserves init status", async () => {
    const response = buildApiSuccessResponse(
      {
        jobId: "abc123",
        status: "queued"
      },
      { status: 202 }
    );

    expect(response.status).toBe(202);
    await expect(response.json()).resolves.toEqual({
      ok: true,
      jobId: "abc123",
      status: "queued"
    });
  });
});

describe("buildApiErrorResponse", () => {
  it("returns default 400 status with required fields", async () => {
    const response = buildApiErrorResponse({ error: "invalid_payload" });

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      ok: false,
      error: "invalid_payload"
    });
  });

  it("includes optional message, issues and details", async () => {
    const response = buildApiErrorResponse({
      error: "validation_failed",
      status: 422,
      message: "Payload did not match schema.",
      issues: [{ path: "items.0.tokenId", message: "Invalid token id" }],
      details: { requestId: "req_123" }
    });

    expect(response.status).toBe(422);
    await expect(response.json()).resolves.toEqual({
      ok: false,
      error: "validation_failed",
      message: "Payload did not match schema.",
      issues: [{ path: "items.0.tokenId", message: "Invalid token id" }],
      requestId: "req_123"
    });
  });
});
