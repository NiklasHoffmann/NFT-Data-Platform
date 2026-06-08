export const dynamic = "force-dynamic";

export async function GET(): Promise<Response> {
  return Response.json(
    {
      ok: true,
      service: "web",
      status: "live"
    },
    {
      status: 200
    }
  );
}
