import { describe, expect, it } from "vitest";
import { isRetryableMediaFailure, toNonEmptyErrorMessage } from "./jobs/processors";

describe("toNonEmptyErrorMessage", () => {
  it("keeps a plain message as it is", () => {
    expect(toNonEmptyErrorMessage(new Error("Media request failed with status 404."), "fallback")).toBe(
      "Media request failed with status 404."
    );
  });

  it("falls back only when there is nothing to describe", () => {
    expect(toNonEmptyErrorMessage(undefined, "fallback")).toBe("fallback");
    expect(toNonEmptyErrorMessage("   ", "fallback")).toBe("fallback");
  });

  it("unwraps the cause that fetch hides its real reason in", () => {
    const cause = new Error("connect ECONNREFUSED 10.0.0.1:443");
    const message = toNonEmptyErrorMessage(new TypeError("fetch failed", { cause }), "fallback");

    expect(message).toContain("fetch failed");
    expect(message).toContain("ECONNREFUSED");
  });

  it("describes an AggregateError by its members rather than its empty message", () => {
    const aggregate = new AggregateError([
      new Error("connect ETIMEDOUT 10.0.0.1:443"),
      new Error("connect ETIMEDOUT 10.0.0.2:443")
    ]);
    const message = toNonEmptyErrorMessage(aggregate, "fallback");

    // The bare name was what made this useless, in the stored detail and in retry
    // classification alike.
    expect(message).not.toBe("AggregateError");
    expect(message).toContain("ETIMEDOUT");
  });

  it("collapses identical members instead of repeating them", () => {
    const aggregate = new AggregateError([
      new Error("connect ECONNREFUSED"),
      new Error("connect ECONNREFUSED")
    ]);

    expect(toNonEmptyErrorMessage(aggregate, "fallback")).toBe("connect ECONNREFUSED");
  });

  it("surfaces a node error code that is not already in the message", () => {
    const error = Object.assign(new Error("request to gateway failed"), { code: "ENOTFOUND" });

    expect(toNonEmptyErrorMessage(error, "fallback")).toContain("ENOTFOUND");
  });

  it("uses the error name when the message is empty", () => {
    expect(toNonEmptyErrorMessage(new Error(""), "fallback")).toBe("Error");
  });

  it("stops walking a cycle instead of recursing forever", () => {
    const outer = new Error("outer");
    const inner = new Error("inner", { cause: outer });
    outer.cause = inner;

    expect(() => toNonEmptyErrorMessage(outer, "fallback")).not.toThrow();
  });
});

describe("isRetryableMediaFailure", () => {
  const sourceUrl = "https://gateway.example/ipfs/bafyexample";

  it("retries a connection the gateway refused or dropped", () => {
    const refused = new TypeError("fetch failed", { cause: new Error("connect ECONNREFUSED 10.0.0.1:443") });
    expect(isRetryableMediaFailure({ error: refused, sourceUrl })).toBe(true);
  });

  it("retries a timed out connection, spelled without a separator", () => {
    const aggregate = new AggregateError([new Error("connect ETIMEDOUT 10.0.0.1:443")]);
    expect(isRetryableMediaFailure({ error: aggregate, sourceUrl })).toBe(true);
  });

  it("does not retry a hostname that does not resolve", () => {
    const missing = new TypeError("x", { cause: new Error("getaddrinfo ENOTFOUND gone.example") });
    expect(isRetryableMediaFailure({ error: missing, sourceUrl })).toBe(false);
  });

  it("does not retry a payload that is simply too large", () => {
    const tooLarge = new Error("Media payload exceeds the 25 MB safety limit.");
    expect(isRetryableMediaFailure({ error: tooLarge, sourceUrl })).toBe(false);
  });

  it("retries a gateway that answers 429 or 504", () => {
    expect(
      isRetryableMediaFailure({ error: new Error("Media request failed with status 429."), sourceUrl })
    ).toBe(true);
    expect(
      isRetryableMediaFailure({ error: new Error("Media request failed with status 504."), sourceUrl })
    ).toBe(true);
  });

  it("does not retry a 403", () => {
    expect(
      isRetryableMediaFailure({ error: new Error("Media request failed with status 403."), sourceUrl })
    ).toBe(false);
  });
});
