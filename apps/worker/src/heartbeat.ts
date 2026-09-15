import { randomUUID } from "node:crypto";
import type { Db } from "mongodb";
import { deleteWorkerHeartbeat, upsertWorkerHeartbeat } from "@nft-platform/db";

export type WorkerHeartbeatConfig = {
  chainIndexingEnabled: boolean;
  chainIndexingPollIntervalMs: number;
  chainIndexingAllowlistSize: number;
  metadataSweepEnabled: boolean;
  metadataSweepIntervalMs: number;
};

/**
 * Chosen to be well below the staleness threshold the health view applies, so a single missed write
 * - a slow Mongo round trip, a restart - does not immediately read as a dead worker.
 */
const heartbeatIntervalMs = 30_000;

/**
 * Publishes the worker's liveness and its background switches into the database, which is the only
 * channel the web service can read them from. Returns the stop function; it removes the record so
 * a clean shutdown shows up as "gone" right away rather than as a worker that went stale.
 */
export function startWorkerHeartbeat(params: {
  database: Db;
  config: WorkerHeartbeatConfig;
}): () => Promise<void> {
  const instanceId = randomUUID();
  const startedAt = new Date();

  const writeHeartbeat = async (): Promise<void> => {
    try {
      await upsertWorkerHeartbeat({
        database: params.database,
        instanceId,
        startedAt,
        lastSeenAt: new Date(),
        ...params.config
      });
    } catch (error) {
      // A failed heartbeat must not take the worker down; job processing is the actual job here.
      console.warn("[worker] heartbeat write failed", { instanceId, error });
    }
  };

  void writeHeartbeat();
  const timer = setInterval(() => {
    void writeHeartbeat();
  }, heartbeatIntervalMs);

  timer.unref?.();

  return async () => {
    clearInterval(timer);

    await deleteWorkerHeartbeat({ database: params.database, instanceId }).catch((error: unknown) => {
      console.warn("[worker] heartbeat cleanup failed", { instanceId, error });
    });
  };
}
