export function extractErrorMessage(request: {
  response_body?: string | null;
  error?: string | null;
}): string {
  const fromBody = (body: string | null | undefined): string | null => {
    if (!body) return null;
    try {
      const parsed = JSON.parse(body);
      if (parsed?.error?.message) return String(parsed.error.message);
    } catch {
    }
    return body;
  };

  const bodyMessage = fromBody(request.response_body);
  if (bodyMessage) return bodyMessage;

  if (request.error) {
    try {
      const reason = JSON.parse(request.error);
      const inner = fromBody(reason?.details?.body);
      if (inner) return inner;
      if (reason?.details) {
        const status = reason.details.status;
        return typeof status === "number" ? `Request failed (${status})` : "Request failed";
      }
    } catch {
    }
    const legacy = request.error.match(/^Upstream returned \d+: ([\s\S]+)$/);
    if (legacy) return fromBody(legacy[1]) ?? legacy[1];
    return fromBody(request.error) ?? request.error;
  }

  return "Request failed";
}
