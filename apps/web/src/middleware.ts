import { type NextRequest, NextResponse } from "next/server";

/**
 * Reject stale or forged Next-Action requests before Next.js tries to
 * resolve them. Without this, an old browser bundle (or a scanner) that
 * sends an unknown action ID triggers an unhandledRejection in Next.js,
 * which makes the server appear unresponsive.
 */
export function middleware(request: NextRequest): NextResponse {
  if (request.headers.has("next-action")) {
    return NextResponse.json(
      { error: "invalid_or_expired_action" },
      { status: 400 }
    );
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"]
};
