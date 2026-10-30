import { describe, expect, it } from "vitest";
import { buildMetadataFetchError } from "./jobs/processors";

describe("buildMetadataFetchError", () => {
  it("reports the source error, not the fallback that was tried after it", () => {
    // The gateway fallbacks are appended after the URI the contract named, and a
    // fallback built from a non-CID identifier is answered with 400 by every gateway.
    // Reporting the last failure buried the real reason under that 400.
    const error = buildMetadataFetchError({
      firstError: new Error("Metadata URI host could not be resolved."),
      lastError: new Error("Metadata request failed with status 400."),
      candidateCount: 3
    });

    expect(error.message).toContain("host could not be resolved");
    expect(error.message).not.toContain("400");
  });

  it("names how many candidates were tried, so the message stays honest", () => {
    const error = buildMetadataFetchError({
      firstError: new Error("Metadata request failed with status 404."),
      lastError: new Error("Metadata request failed with status 400."),
      candidateCount: 3
    });

    expect(error.message).toBe("Metadata request failed with status 404. (3 candidates tried)");
  });

  it("leaves a single-candidate message untouched", () => {
    const error = buildMetadataFetchError({
      firstError: new Error("Metadata request failed with status 404."),
      lastError: new Error("Metadata request failed with status 404."),
      candidateCount: 1
    });

    expect(error.message).toBe("Metadata request failed with status 404.");
  });

  it("falls back to the last error when the first was never recorded", () => {
    const error = buildMetadataFetchError({
      firstError: null,
      lastError: new Error("Metadata request timed out after 10000 ms."),
      candidateCount: 1
    });

    expect(error.message).toBe("Metadata request timed out after 10000 ms.");
  });

  it("still says something when nothing was recorded at all", () => {
    const error = buildMetadataFetchError({ firstError: null, lastError: null, candidateCount: 0 });

    expect(error.message).toContain("could not be fetched");
  });
});
