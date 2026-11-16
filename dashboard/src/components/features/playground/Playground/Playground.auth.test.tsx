import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import OpenAI from "openai";

describe("Playground OpenAI Client Authentication", () => {
  let fetchSpy: any;
  let originalFetch: typeof fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
    fetchSpy = vi
      .spyOn(globalThis, "fetch")
      .mockImplementation(async (url, _init) => {
        if (url.toString().includes("/chat/completions")) {
          const encoder = new TextEncoder();
          const stream = new ReadableStream({
            async start(controller) {
              controller.enqueue(
                encoder.encode(
                  'data: {"choices":[{"delta":{"content":"test"}}]}\n\n',
                ),
              );
              controller.enqueue(encoder.encode("data: [DONE]\n\n"));
              controller.close();
            },
          });

          return new Response(stream, {
            status: 200,
            headers: { "Content-Type": "text/event-stream" },
          });
        }

        return new Response("{}", {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      });
  });

  afterEach(() => {
    fetchSpy.mockRestore();
    globalThis.fetch = originalFetch;
  });

  it("does not send Authorization header when defaultHeaders overrides it", async () => {
    const openai = new OpenAI({
      baseURL: "http://localhost:3000/admin/api/v1/ai/v1",
      apiKey: "test-api-key",
      dangerouslyAllowBrowser: true,
      defaultHeaders: {
        Authorization: null as any,
      },
    });

    try {
      await openai.chat.completions.create({
        model: "test-model",
        messages: [{ role: "user", content: "test" }],
        stream: true,
      });
    } catch {
    }

    expect(fetchSpy).toHaveBeenCalled();

    const openaiCalls = fetchSpy.mock.calls.filter((call: any) =>
      call[0]?.toString().includes("/chat/completions"),
    );

    expect(openaiCalls.length).toBeGreaterThan(0);

    const [_url, requestInit] = openaiCalls[0] as [
      unknown,
      RequestInit | undefined,
    ];
    const headers = new Headers(requestInit?.headers);

    expect(headers.has("Authorization")).toBe(false);
    expect(headers.get("Authorization")).toBe(null);

    const allHeaders: Record<string, string> = {};
    headers.forEach((value, key) => {
      allHeaders[key] = value;
    });
    console.log("All headers sent:", JSON.stringify(allHeaders, null, 2));
  });

  it("sends Authorization header when apiKey is provided", async () => {
    const openai = new OpenAI({
      baseURL: "http://localhost:3000/admin/api/v1/ai/v1",
      apiKey: "test-api-key",
      dangerouslyAllowBrowser: true,
    });

    try {
      await openai.chat.completions.create({
        model: "test-model",
        messages: [{ role: "user", content: "test" }],
        stream: true,
      });
    } catch {
    }

    expect(fetchSpy).toHaveBeenCalled();

    const openaiCalls = fetchSpy.mock.calls.filter((call: any) =>
      call[0]?.toString().includes("/chat/completions"),
    );

    expect(openaiCalls.length).toBeGreaterThan(0);

    const [_url, requestInit] = openaiCalls[0] as [
      unknown,
      RequestInit | undefined,
    ];
    const headers = new Headers(requestInit?.headers);

    expect(headers.has("Authorization")).toBe(true);
    expect(headers.get("Authorization")).toBe("Bearer test-api-key");
  });
});
