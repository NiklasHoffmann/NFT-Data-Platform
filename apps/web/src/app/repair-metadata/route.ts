import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";
import { buildRepairMetadataRedirectUrl, handleRepairMetadataSubmission } from "../repair-metadata-flow";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest): Promise<Response> {
  const formData = await request.formData();
  const redirectUrl = buildRepairMetadataRedirectUrl(await handleRepairMetadataSubmission(formData));

  // 303 so the browser follows with GET and a reload does not re-submit the form.
  return new NextResponse(null, {
    status: 303,
    headers: {
      Location: redirectUrl
    }
  });
}
