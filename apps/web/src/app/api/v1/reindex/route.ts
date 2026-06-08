import type { NextRequest } from "next/server";
import { reindexRangeJobSchema } from "@nft-platform/queue";
import { withAuthenticatedRoute } from "../../../../lib/api-auth";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../lib/api-validation";
import { guardContractAddress } from "../../../../lib/contract-address-guard";
import { getWebMongoDatabase } from "../../../../lib/mongodb";
import { enqueueReindexRangeJob } from "../../../../lib/queue";
import { persistQueueBackedJobRecord } from "../../../../services/jobs/queue-job-service";

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
  const contractGuardResult = await guardContractAddress({
    chainId: validatedPayload.chainId,
    contractAddress: validatedPayload.contractAddress
  });

  if (!contractGuardResult.ok) {
    return buildApiErrorResponse({
      error: "invalid_contract_address",
      message: contractGuardResult.message,
      status: 400
    });
  }

  const timestamp = new Date();
  const database = getWebMongoDatabase();
  const queuedJob = await enqueueReindexRangeJob(validatedPayload);
  const persistedJob = await persistQueueBackedJobRecord({
    database,
    queuedJob,
    type: "reindex-range",
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