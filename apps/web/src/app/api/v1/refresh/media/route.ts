import type { NextRequest } from "next/server";
import { refreshMediaJobSchema } from "@nft-platform/queue";
import { withAuthenticatedRoute } from "../../../../../lib/api-auth";
import { buildApiSuccessResponse } from "../../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../../lib/api-validation";
import { getWebMongoDatabase } from "../../../../../lib/mongodb";
import { enqueueRefreshMediaJob } from "../../../../../lib/queue";
import { persistQueueBackedJobRecord } from "../../../../../services/jobs/queue-job-service";

export const dynamic = "force-dynamic";

const postHandler = withAuthenticatedRoute(["refresh:media"], async ({ auth }) => {
  const parsedBody = safeParseJsonRequestBody(auth.bodyText);

  if (!parsedBody.ok) {
    return parsedBody.response;
  }

  const validatedPayloadResult = refreshMediaJobSchema.safeParse(parsedBody.data);

  if (!validatedPayloadResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_refresh_media_request",
      issues: buildValidationIssues(validatedPayloadResult.error)
    });
  }

  const validatedPayload = validatedPayloadResult.data;
  const timestamp = new Date();
  const database = getWebMongoDatabase();
  const queuedJob = await enqueueRefreshMediaJob(validatedPayload);
  const persistedJob = await persistQueueBackedJobRecord({
    database,
    queuedJob,
    type: "refresh-media",
    payload: validatedPayload,
    now: timestamp
  });

  return buildApiSuccessResponse(
    {
      jobId: persistedJob.jobId,
      queueJobId: queuedJob.jobId,
      status: queuedJob.status
    },
    { status: persistedJob.statusCode }
  );
});

export async function POST(request: NextRequest): Promise<Response> {
  return postHandler(request, undefined);
}