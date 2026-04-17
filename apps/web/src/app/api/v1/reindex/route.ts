import type { NextRequest } from "next/server";
import { createJob } from "@nft-platform/db";
import { reindexRangeJobSchema } from "@nft-platform/queue";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { enqueueReindexRangeJob } from "../../../../lib/queue";

export const dynamic = "force-dynamic";

const postHandler = withAuthenticatedRoute(["reindex:write"], async ({ auth }) => {
  const payload = parseJsonBody(auth.bodyText);
  const validatedPayload = reindexRangeJobSchema.parse(payload);
  const timestamp = new Date();
  const database = getWebMongoDatabase();
  const queuedJob = await enqueueReindexRangeJob(validatedPayload);
  const jobId = await createJob(database, {
    queueJobId: queuedJob.jobId,
    type: "reindex-range",
    payload: validatedPayload,
    status: queuedJob.status,
    attempts: queuedJob.attempts,
    lastError: queuedJob.lastError,
    createdAt: timestamp,
    updatedAt: timestamp
  });

  return Response.json(
    {
      ok: true,
      jobId: jobId.toHexString(),
      queueJobId: queuedJob.jobId,
      status: queuedJob.status
    },
    { status: queuedJob.status === "queued" || queuedJob.status === "running" ? 202 : 200 }
  );
});

function parseJsonBody(bodyText: string): unknown {
  if (!bodyText.trim()) {
    return {};
  }

  return JSON.parse(bodyText);
}

export async function POST(request: NextRequest): Promise<Response> {
  return postHandler(request, undefined);
}