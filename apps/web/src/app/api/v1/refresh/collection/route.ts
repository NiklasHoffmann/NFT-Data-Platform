import type { NextRequest } from "next/server";
import { createJob } from "@nft-platform/db";
import { refreshCollectionJobSchema } from "@nft-platform/queue";
import { withAuthenticatedRoute } from "../../../../../lib/api-auth";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../../lib/mongodb";
import { enqueueRefreshCollectionJob } from "../../../../../lib/queue";

export const dynamic = "force-dynamic";

const postHandler = withAuthenticatedRoute(["refresh:collection"], async ({ auth }) => {
  const parsedBody = safeParseJsonRequestBody(auth.bodyText);

  if (!parsedBody.ok) {
    return parsedBody.response;
  }

  const validatedPayloadResult = refreshCollectionJobSchema.safeParse(parsedBody.data);

  if (!validatedPayloadResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_refresh_collection_request",
      issues: buildValidationIssues(validatedPayloadResult.error)
    });
  }

  const validatedPayload = validatedPayloadResult.data;
  const timestamp = new Date();
  const database = getWebMongoDatabase();
  const queuedJob = await enqueueRefreshCollectionJob(validatedPayload);
  const jobId = await createJob(database, {
    queueJobId: queuedJob.jobId,
    type: "refresh-collection",
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