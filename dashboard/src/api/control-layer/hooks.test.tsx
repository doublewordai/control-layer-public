import { describe, it, expect, beforeEach, vi } from "vitest";
import { renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useModels, useModel, useEndpoints, useEndpoint } from "./hooks";
import { dwctlApi } from "./client";
import type { Model, Endpoint } from "./types";

vi.mock("./client", () => ({
  dwctlApi: {
    models: {
      list: vi.fn(),
      get: vi.fn(),
    },
    endpoints: {
      list: vi.fn(),
      get: vi.fn(),
    },
  },
}));

const mockModels: Model[] = [
  {
    id: "model-1",
    alias: "gpt-4",
    model_name: "gpt-4-turbo",
    hosted_on: "endpoint-1",
    description: "Test model 1",
    model_type: "CHAT",
    capabilities: ["chat"],
  },
  {
    id: "model-2",
    alias: "claude",
    model_name: "claude-3-opus",
    hosted_on: "endpoint-2",
    description: "Test model 2",
    model_type: "CHAT",
    capabilities: ["chat"],
  },
];

const mockModelDetail: Model = {
  ...mockModels[0],
  groups: [{ id: "group-1", name: "Test Group", source: "user" }],
  metrics: {
    total_requests: 100,
    total_input_tokens: 1000,
    total_output_tokens: 2000,
    avg_latency_ms: 150,
    last_active_at: "2024-01-01T00:00:00Z",
  },
};

const mockEndpoints: Endpoint[] = [
  {
    id: "endpoint-1",
    name: "OpenAI",
    url: "https://api.openai.com/v1",
    created_at: "2024-01-01T00:00:00Z",
    updated_at: "2024-01-01T00:00:00Z",
    created_by: "user-1",
    requires_api_key: true,
    auth_header_name: "Authorization",
    auth_header_prefix: "Bearer ",
  },
  {
    id: "endpoint-2",
    name: "Anthropic",
    url: "https://api.anthropic.com/v1",
    created_at: "2024-01-01T00:00:00Z",
    updated_at: "2024-01-01T00:00:00Z",
    created_by: "user-1",
    requires_api_key: true,
    auth_header_name: "Authorization",
    auth_header_prefix: "Bearer ",
  },
];

describe("Model cache optimization", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });
    vi.clearAllMocks();
  });

  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  it("should populate individual model cache when fetching list", async () => {
    vi.mocked(dwctlApi.models.list).mockResolvedValue({
      data: mockModels,
      skip: 0,
      limit: 10,
      total_count: mockModels.length,
    });

    const { result } = renderHook(() => useModels({ limit: 10 }), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data?.data).toEqual(mockModels);

    await waitFor(() => {
      const cachedModel1 = queryClient.getQueryData([
        "models",
        "byId",
        "model-1",
        undefined,
      ]);
      return cachedModel1 !== undefined;
    });

    const cachedModel1 = queryClient.getQueryData([
      "models",
      "byId",
      "model-1",
      undefined,
    ]);
    const cachedModel2 = queryClient.getQueryData([
      "models",
      "byId",
      "model-2",
      undefined,
    ]);

    expect(cachedModel1).toEqual(mockModels[0]);
    expect(cachedModel2).toEqual(mockModels[1]);
  });

  it("should use cached data as placeholder then fetch detailed data", async () => {
    vi.mocked(dwctlApi.models.list).mockResolvedValue({
      data: mockModels,
      skip: 0,
      limit: 10,
      total_count: mockModels.length,
    });

    const { result: listResult } = renderHook(() => useModels({ limit: 10 }), {
      wrapper,
    });

    await waitFor(() => expect(listResult.current.isSuccess).toBe(true));

    await waitFor(() => {
      const cachedModel = queryClient.getQueryData([
        "models",
        "byId",
        "model-1",
        undefined,
      ]);
      return cachedModel !== undefined;
    });

    const cachedBasicModel = queryClient.getQueryData([
      "models",
      "byId",
      "model-1",
      undefined,
    ]) as Model;
    expect(cachedBasicModel.id).toBe("model-1");
    expect(cachedBasicModel.groups).toBeUndefined();
    expect(cachedBasicModel.metrics).toBeUndefined();

    vi.mocked(dwctlApi.models.get).mockResolvedValue(mockModelDetail);

    const { result: detailResult } = renderHook(
      () => useModel("model-1", { include: "groups,metrics" }),
      {
        wrapper,
      },
    );

    expect(detailResult.current.data).toBeDefined();
    expect(detailResult.current.data?.id).toBe("model-1");
    expect(detailResult.current.data?.alias).toBe("gpt-4");
    expect(detailResult.current.data?.groups).toBeUndefined();

    await waitFor(() => {
      return vi.mocked(dwctlApi.models.get).mock.calls.length > 0;
    });

    expect(dwctlApi.models.get).toHaveBeenCalledWith("model-1", {
      include: "groups,metrics",
    });

    await waitFor(() => {
      return (
        detailResult.current.isSuccess &&
        detailResult.current.data?.groups !== undefined
      );
    });

    expect(detailResult.current.data?.id).toBe("model-1");
    expect(detailResult.current.data?.groups).toBeDefined();
    expect(detailResult.current.data?.metrics).toBeDefined();
    expect(detailResult.current.data?.groups?.[0]?.name).toBe("Test Group");
    expect(detailResult.current.data?.metrics?.total_requests).toBe(100);
  });

  it("should use populated cache as placeholder", async () => {
    vi.mocked(dwctlApi.models.list).mockResolvedValue({
      data: mockModels,
      skip: 0,
      limit: 10,
      total_count: mockModels.length,
    });

    const { result: listResult } = renderHook(() => useModels({ limit: 10 }), {
      wrapper,
    });

    await waitFor(() => expect(listResult.current.isSuccess).toBe(true));

    await waitFor(() => {
      const cached = queryClient.getQueryData([
        "models",
        "byId",
        "model-2",
        undefined,
      ]);
      return cached !== undefined;
    });

    vi.mocked(dwctlApi.models.get).mockResolvedValue({
      ...mockModels[1],
      groups: [],
    });

    const { result: detailResult } = renderHook(() => useModel("model-2"), {
      wrapper,
    });

    expect(detailResult.current.data).toEqual(mockModels[1]);
  });

  it("should not use placeholder data if model not in any cache", async () => {
    vi.mocked(dwctlApi.models.get).mockResolvedValue(mockModelDetail);

    const { result } = renderHook(() => useModel("model-1"), { wrapper });

    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(true);

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toEqual(mockModelDetail);
  });

  it("should handle multiple pagination queries and still find cached models", async () => {
    vi.mocked(dwctlApi.models.list).mockResolvedValue({
      data: [mockModels[0]],
      skip: 0,
      limit: 10,
      total_count: 2,
    });

    const { result: page1Result } = renderHook(
      () => useModels({ skip: 0, limit: 1 }),
      { wrapper },
    );

    await waitFor(() => expect(page1Result.current.isSuccess).toBe(true));

    vi.mocked(dwctlApi.models.list).mockResolvedValue({
      data: [mockModels[1]],
      skip: 0,
      limit: 10,
      total_count: 2,
    });

    const { result: page2Result } = renderHook(
      () => useModels({ skip: 1, limit: 1 }),
      { wrapper },
    );

    await waitFor(() => expect(page2Result.current.isSuccess).toBe(true));

    vi.mocked(dwctlApi.models.get).mockResolvedValue({
      ...mockModels[1],
      groups: [],
    });

    const { result: detailResult } = renderHook(() => useModel("model-2"), {
      wrapper,
    });

    expect(detailResult.current.data).toEqual(mockModels[1]);
  });
});

describe("Endpoint cache optimization", () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });
    vi.clearAllMocks();
  });

  const wrapper = ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  it("should populate individual endpoint cache when fetching list", async () => {
    vi.mocked(dwctlApi.endpoints.list).mockResolvedValue(mockEndpoints);

    const { result } = renderHook(() => useEndpoints(), { wrapper });

    await waitFor(() => expect(result.current.isSuccess).toBe(true));

    expect(result.current.data).toEqual(mockEndpoints);

    await waitFor(() => {
      const cachedEndpoint1 = queryClient.getQueryData([
        "endpoints",
        "byId",
        "endpoint-1",
      ]);
      return cachedEndpoint1 !== undefined;
    });

    const cachedEndpoint1 = queryClient.getQueryData([
      "endpoints",
      "byId",
      "endpoint-1",
    ]);
    const cachedEndpoint2 = queryClient.getQueryData([
      "endpoints",
      "byId",
      "endpoint-2",
    ]);

    expect(cachedEndpoint1).toEqual(mockEndpoints[0]);
    expect(cachedEndpoint2).toEqual(mockEndpoints[1]);
  });

  it("should use cached data as placeholder when fetching individual endpoint", async () => {
    vi.mocked(dwctlApi.endpoints.list).mockResolvedValue(mockEndpoints);

    const { result: listResult } = renderHook(() => useEndpoints(), {
      wrapper,
    });

    await waitFor(() => expect(listResult.current.isSuccess).toBe(true));

    await waitFor(() => {
      const cachedEndpoint = queryClient.getQueryData([
        "endpoints",
        "byId",
        "endpoint-1",
      ]);
      return cachedEndpoint !== undefined;
    });

    vi.mocked(dwctlApi.endpoints.get).mockResolvedValue(mockEndpoints[0]);

    const { result: detailResult } = renderHook(
      () => useEndpoint("endpoint-1"),
      {
        wrapper,
      },
    );

    expect(detailResult.current.data).toBeDefined();
    expect(detailResult.current.data?.id).toBe("endpoint-1");
    expect(detailResult.current.data?.name).toBe("OpenAI");

    await waitFor(() => detailResult.current.isSuccess);

    expect(detailResult.current.data?.id).toBe("endpoint-1");

    expect(dwctlApi.endpoints.get).toHaveBeenCalledWith("endpoint-1");
  });

  it("should respect enabled option in useEndpoint", async () => {
    vi.mocked(dwctlApi.endpoints.get).mockResolvedValue(mockEndpoints[0]);

    const { result } = renderHook(
      () => useEndpoint("endpoint-1", { enabled: false }),
      { wrapper },
    );

    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(dwctlApi.endpoints.get).not.toHaveBeenCalled();
    expect(result.current.data).toBeUndefined();
    expect(result.current.isLoading).toBe(false);
  });

  it("should use populated cache as placeholder", async () => {
    vi.mocked(dwctlApi.endpoints.list).mockResolvedValue(mockEndpoints);

    const { result: listResult } = renderHook(() => useEndpoints(), {
      wrapper,
    });

    await waitFor(() => expect(listResult.current.isSuccess).toBe(true));

    await waitFor(() => {
      const cached = queryClient.getQueryData([
        "endpoints",
        "byId",
        "endpoint-2",
      ]);
      return cached !== undefined;
    });

    vi.mocked(dwctlApi.endpoints.get).mockResolvedValue(mockEndpoints[1]);

    const { result: detailResult } = renderHook(
      () => useEndpoint("endpoint-2"),
      { wrapper },
    );

    expect(detailResult.current.data).toEqual(mockEndpoints[1]);
  });
});
