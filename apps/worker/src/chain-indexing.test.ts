import { describe, expect, it } from "vitest";
import { resolveReindexWindow } from "./chain-indexing";

const confirmations = 12;
const maxBlockRange = 2000;

describe("resolveReindexWindow", () => {
  it("stays a full confirmation depth behind the chain head", () => {
    const window = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: 900,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(window).toEqual({ fromBlock: 901, toBlock: 988 });
  });

  it("starts at the deploy block when nothing has been indexed yet", () => {
    const window = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: null,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(window).toEqual({ fromBlock: 100, toBlock: 988 });
  });

  it("queues nothing while the unconfirmed head is all that is left", () => {
    // Indexed up to 990, head at 1000: everything above 988 is still reorg-prone.
    const window = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: 990,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(window).toBeNull();
  });

  it("queues the boundary block exactly once it is confirmed", () => {
    const atBoundary = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: 987,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(atBoundary).toEqual({ fromBlock: 988, toBlock: 988 });

    const justBefore = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: 988,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(justBefore).toBeNull();
  });

  it("caps the window at the configured block range", () => {
    const window = resolveReindexWindow({
      latestBlock: 100_000,
      lastIndexedBlock: 900,
      deployBlock: 100,
      confirmations,
      maxBlockRange
    });

    expect(window).toEqual({ fromBlock: 901, toBlock: 2900 });
  });

  it("queues nothing when the collection has no known starting block", () => {
    const window = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: null,
      deployBlock: null,
      confirmations,
      maxBlockRange
    });

    expect(window).toBeNull();
  });

  it("never produces a negative window on a chain younger than the confirmation depth", () => {
    const window = resolveReindexWindow({
      latestBlock: 5,
      lastIndexedBlock: null,
      deployBlock: 0,
      confirmations,
      maxBlockRange
    });

    expect(window).toEqual({ fromBlock: 0, toBlock: 0 });
  });

  it("indexes up to the head when confirmations are disabled", () => {
    const window = resolveReindexWindow({
      latestBlock: 1000,
      lastIndexedBlock: 900,
      deployBlock: 100,
      confirmations: 0,
      maxBlockRange
    });

    expect(window).toEqual({ fromBlock: 901, toBlock: 1000 });
  });
});
