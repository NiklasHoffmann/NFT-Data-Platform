import { isTerminalMetadataError, terminalMetadataErrorPatterns } from "@nft-platform/db";
import { describe, expect, it } from "vitest";

describe("isTerminalMetadataError", () => {
  it("treats a document that is gone as terminal", () => {
    expect(isTerminalMetadataError("Metadata request failed with status 404.")).toBe(true);
    expect(isTerminalMetadataError("Metadata request failed with status 410.")).toBe(true);
  });

  it("treats a host that does not exist as terminal", () => {
    expect(isTerminalMetadataError("Metadata URI host could not be resolved.")).toBe(true);
  });

  it("keeps anything that could come out differently on a retry", () => {
    // These are the cases where a second attempt genuinely helps, so they must stay
    // in the repair backlog rather than being filed away as hopeless.
    expect(isTerminalMetadataError("Metadata request timed out after 10000 ms.")).toBe(false);
    expect(isTerminalMetadataError("Metadata request failed with status 429.")).toBe(false);
    expect(isTerminalMetadataError("Metadata request failed with status 503.")).toBe(false);
    expect(isTerminalMetadataError("fetch failed: connect ECONNREFUSED 10.0.0.1:443")).toBe(false);
  });

  it("does not file away a malformed document, which is a parsing problem", () => {
    expect(isTerminalMetadataError("Unexpected token < in JSON at position 0")).toBe(false);
  });

  it("still matches when the fallback candidate count is appended", () => {
    // buildMetadataFetchError appends "(3 candidates tried)" once fallbacks ran.
    expect(isTerminalMetadataError("Metadata request failed with status 404. (3 candidates tried)")).toBe(
      true
    );
  });

  it("handles a missing message", () => {
    expect(isTerminalMetadataError(null)).toBe(false);
    expect(isTerminalMetadataError(undefined)).toBe(false);
    expect(isTerminalMetadataError("")).toBe(false);
  });

  it("exposes patterns that are safe to reuse as Mongo filters", () => {
    // The filter clones each pattern; a global flag would make the shared lastIndex
    // leak between calls and drop matches.
    for (const pattern of terminalMetadataErrorPatterns) {
      expect(pattern.global).toBe(false);
    }
  });
});
