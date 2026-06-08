import type { NextRequest } from "next/server";
import { createJob } from "@nft-platform/db";
import { refreshTokenJobSchema } from "@nft-platform/queue";
import { getWebMongoDatabase } from "../../../../../lib/mongodb";
import { withAuthenticatedRoute } from "../../../../../lib/api-auth";
import { buildValidationErrorResponse, buildValidationIssues, safeParseJsonRequestBody } from "../../../../../lib/api-validation";
import { guardContractAddress } from "../../../../../lib/contract-address-guard";
import { enqueueRefreshTokenJob } from "../../../../../lib/queue";

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
    return Response.json(
      {
        ok: false,
        error: "invalid_contract_address",
        message: contractGuardResult.message
      },
      { status: 400 }
    );
  }

  const timestamp = new Date();
  const database = getWebMongoDatabase();
  const queuedJob = await enqueueRefreshTokenJob(validatedPayload);
  const jobId = await createJob(database, {
    queueJobId: queuedJob.jobId,
    type: "refresh-token",
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