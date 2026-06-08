export type ApiValidationIssue = {
  path: string;
  message: string;
};

export function buildApiSuccessResponse<T extends Record<string, unknown>>(
  payload: T,
  init?: ResponseInit
): Response {
  return Response.json(
    {
      ok: true,
      ...payload
    },
    init
  );
}

export function buildApiErrorResponse(params: {
  error: string;
  status?: number;
  message?: string;
  issues?: ApiValidationIssue[];
  details?: Record<string, unknown>;
}): Response {
  return Response.json(
    {
      ok: false,
      error: params.error,
      ...(params.message ? { message: params.message } : {}),
      ...(params.issues ? { issues: params.issues } : {}),
      ...(params.details ? params.details : {})
    },
    {
      status: params.status ?? 400
    }
  );
}