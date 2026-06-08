import type { NextRequest } from "next/server";
import { refreshTokenJobSchema } from "@nft-platform/queue";
import { getWebMongoDatabase } from "../../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../../lib/api-auth";
import { buildApiErrorResponse, buildApiSuccessResponse } from "../../../../../lib/api-response";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../../lib/api-validation";
import { guardContractAddress } from "../../../../../lib/contract-address-guard";
import { enqueueRefreshTokenJob } from "../../../../../lib/queue";
import { persistQueueBackedJobRecord } from "../../../../../services/jobs/queue-job-service";

export const dynamic = "force-dynamic";

const postHandler = withAuthenticatedRoute(["refresh:token"], async ({ auth }) => {
  const parsedBody = safeParseJsonRequestBody(auth.bodyText);

  if (!parsedBody.ok) {
    return parsedBody.response;
  }

  const validatedPayloadResult = refreshTokenJobSchema.safeParse(parsedBody.data);

  if (!validatedPayloadResult.success) {
    return buildValidationErrorResponse({
      error: "invalid_refresh_token_request",
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
  const queuedJob = await enqueueRefreshTokenJob(validatedPayload);
  const persistedJob = await persistQueueBackedJobRecord({
    database,
    queuedJob,
    type: "refresh-token",
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