import type { NextRequest } from "next/server";
import { NextResponse } from "next/server";
import { buildRemirrorRedirectUrl, handleRemirrorSubmission } from "../remirror-flow";

export const dynamic = "force-dynamic";

export async function POST(_request: NextRequest): Promise<Response> {
  const redirectUrl = buildRemirrorRedirectUrl(await handleRemirrorSubmission());

  // 303 so the browser follows with GET and a reload does not re-submit the form.
  return new NextResponse(null, {
    status: 303,
    headers: {
      Location: redirectUrl
    }
  });
}
