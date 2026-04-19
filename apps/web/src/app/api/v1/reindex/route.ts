import type { NextRequest } from "next/server";
import { createJob } from "@nft-platform/db";
import { reindexRangeJobSchema } from "@nft-platform/queue";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { enqueueReindexRangeJob } from "../../../../lib/queue";

export const dynamic = "force-dynamic";

const postHandler = withAuthenticatedRoute(["reindex:write"], async ({ auth }) => {
  const parsedBody = safeParseJsonRequestBody(auth.bodyText);

  if (!parsedBody.ok) {
    return parsedBody.response;
  }

  const validatedPayloadResult = reindexRangeJobSchema.safeParse(parsedBody.data);

  if (!validatedPayloadResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_reindex_request",
      issues: buildValidationIssues(validatedPayloadResult.error)
    });
  }

  const validatedPayload = validatedPayloadResult.data;
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

export async function POST(request: NextRequest): Promise<Response> {
  return postHandler(request, undefined);
}