import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";
import { buildDiscoverRedirectUrl, handleDiscoverSubmission } from "../discover-flow";

export const dynamic = "force-dynamic";

export async function POST(request: NextRequest): Promise<Response> {
  const formData = await request.formData();
  const redirectUrl = buildDiscoverRedirectUrl(await handleDiscoverSubmission(formData));

  return new NextResponse(null, {
    status: 303,
    headers: {
      Location: redirectUrl
    }
  });
}