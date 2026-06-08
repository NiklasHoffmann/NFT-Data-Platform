import { ObjectId } from "mongodb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createJob } from "@nft-platform/db";
import {
  mapQueueBackedDiscoveryStatus,
  persistQueueBackedJobRecord
} from "./queue-job-service";

vi.mock("@nft-platform/db", () => ({
  createJob: vi.fn()
}));

describe("persistQueueBackedJobRecord", () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it("persists the queue-backed job and maps queued status to HTTP 202", async () => {
    const now = new Date("2026-06-08T20:00:00.000Z");
    const persistedId = new ObjectId();
    vi.mocked(createJob).mockResolvedValueOnce(persistedId);

    const result = await persistQueueBackedJobRecord({
      database: {} as never,
      queuedJob: {
        jobId: "queue-job-123",
        status: "queued",
        attempts: 2,
        lastError: null
      },
      type: "refresh-token",
      payload: {
        chainId: 1,
        contractAddress: "0xabc",
        tokenId: "42"
      },
      now
    });

    expect(createJob).toHaveBeenCalledTimes(1);
    expect(createJob).toHaveBeenCalledWith(
      {},
      {
        queueJobId: "queue-job-123",
        type: "refresh-token",
        payload: {
          chainId: 1,
          contractAddress: "0xabc",
          tokenId: "42"
        },
        status: "queued",
        attempts: 2,
        lastError: null,
        createdAt: now,
        updatedAt: now
      }
    );
    expect(result).toEqual({
      jobId: persistedId.toHexString(),
      statusCode: 202
    });
  });

  it("maps running status to HTTP 202", async () => {
    vi.mocked(createJob).mockResolvedValueOnce(new ObjectId());

    const result = await persistQueueBackedJobRecord({
      database: {} as never,
      queuedJob: {
        jobId: "queue-job-running",
        status: "running",
        attempts: 0,
        lastError: null
      },
      type: "refresh-media",
      payload: {},
      now: new Date("2026-06-08T20:01:00.000Z")
    });

    expect(result.statusCode).toBe(202);
  });

  it("maps done and failed statuses to HTTP 200", async () => {
    vi.mocked(createJob)
      .mockResolvedValueOnce(new ObjectId())
      .mockResolvedValueOnce(new ObjectId());

    const doneResult = await persistQueueBackedJobRecord({
      database: {} as never,
      queuedJob: {
        jobId: "queue-job-done",
        status: "done",
        attempts: 1,
        lastError: null
      },
      type: "refresh-collection",
      payload: {},
      now: new Date("2026-06-08T20:02:00.000Z")
    });

    const failedResult = await persistQueueBackedJobRecord({
      database: {} as never,
      queuedJob: {
        jobId: "queue-job-failed",
        status: "failed",
        attempts: 3,
        lastError: "boom"
      },
      type: "reindex-range",
      payload: {},
      now: new Date("2026-06-08T20:03:00.000Z")
    });

    expect(doneResult.statusCode).toBe(200);
    expect(failedResult.statusCode).toBe(200);
  });
});

describe("mapQueueBackedDiscoveryStatus", () => {
  it("maps only failed to failed and all other statuses to queued", () => {
    expect(mapQueueBackedDiscoveryStatus("failed")).toBe("failed");
    expect(mapQueueBackedDiscoveryStatus("queued")).toBe("queued");
    expect(mapQueueBackedDiscoveryStatus("running")).toBe("queued");
    expect(mapQueueBackedDiscoveryStatus("done")).toBe("queued");
  });
});
