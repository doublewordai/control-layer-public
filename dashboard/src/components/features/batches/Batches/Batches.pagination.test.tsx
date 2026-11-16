import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { Batches } from "./Batches";
import * as hooks from "../../../../api/control-layer/hooks";

vi.mock("../../../../api/control-layer/hooks", () => ({
  useConfig: vi.fn(),
  useFiles: vi.fn(),
  useBatches: vi.fn(),
  useDeleteFile: vi.fn(),
  useCancelBatch: vi.fn(),
  useBatchAnalytics: vi.fn(() => ({
    data: null,
    isLoading: false,
  })),
  useFileCostEstimate: vi.fn(() => ({
    data: null,
    isLoading: false,
    error: null,
  })),
  useUser: vi.fn(() => ({
    data: { roles: ["PlatformManager"] },
    isLoading: false,
  })),
  useOrganizationMembers: vi.fn(() => ({
    data: [],
    isLoading: false,
  })),
  useUsers: vi.fn(() => ({
    data: { data: [], total_count: 0 },
    isLoading: false,
  })),
}));

vi.mock("../../../../utils/authorization", () => ({
  useAuthorization: vi.fn(() => ({
    userRoles: ["PlatformManager"],
    isLoading: false,
    hasPermission: () => true,
    canAccessRoute: () => true,
    getFirstAccessibleRoute: () => "/batches",
  })),
}));

vi.mock("../../../../contexts/organization/useOrganizationContext", () => ({
  useOrganizationContext: vi.fn(() => ({
    activeOrganizationId: null,
    activeOrganization: null,
    isOrgContext: false,
    setActiveOrganization: vi.fn(),
  })),
}));

vi.mock("../../../modals/CreateFileModal", () => ({
  UploadFileModal: () => null,
}));

vi.mock("../../../modals/CreateBatchModal", () => ({
  CreateBatchModal: () => null,
}));

vi.mock("../../../modals/DownloadFileModal", () => ({
  DownloadFileModal: () => null,
}));

vi.mock("../../../modals", () => ({
  ApiExamples: () => null,
}));

vi.mock("sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

const createMockFiles = (pageNum: number, count: number) => {
  return Array.from({ length: count }, (_, i) => ({
    id: `file-page${pageNum}-${i}`,
    object: "file",
    bytes: 100000 + i * 1000,
    created_at: 1730995200 + i * 86400,
    expires_at: 1765065600 + i * 86400,
    filename: `file_page${pageNum}_${i}.jsonl`,
    purpose: "batch",
  }));
};

const createMockBatches = (pageNum: number, count: number) => {
  return Array.from({ length: count }, (_, i) => ({
    id: `batch-page${pageNum}-${i}`,
    object: "batch",
    endpoint: "/v1/chat/completions",
    errors: null,
    input_file_id: `file-${i}`,
    completion_window: "24h",
    status: "completed" as const,
    output_file_id: `file-output-${i}`,
    error_file_id: null,
    created_at: 1730822400 + i * 86400,
    in_progress_at: 1730824200 + i * 86400,
    expires_at: 1731427200 + i * 86400,
    finalizing_at: 1730865600 + i * 86400,
    completed_at: 1730869200 + i * 86400,
    failed_at: null,
    expired_at: null,
    cancelling_at: null,
    cancelled_at: null,
    request_counts: {
      total: 100,
      completed: 98,
      failed: 2,
    },
    metadata: {},
  }));
};

const defaultProps = {
  onOpenUploadModal: vi.fn(),
  onOpenCreateBatchModal: vi.fn(),
  onOpenDownloadModal: vi.fn(),
  onOpenDeleteDialog: vi.fn(),
  onOpenDeleteBatchDialog: vi.fn(),
  onOpenCancelDialog: vi.fn(),
  onBatchCreatedCallback: vi.fn(),
};

const createWrapper = (initialEntries = ["/"]) => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
      mutations: { retry: false },
    },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <MemoryRouter initialEntries={initialEntries}>
      <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
    </MemoryRouter>
  );
};

describe("Batches - Pagination", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(hooks.useConfig).mockReturnValue({
      data: {
        batches: {
          allowed_completion_windows: ["24h", "1h"],
          async_requests: { enabled: true, completion_window: "1h" },
        },
      },
      isLoading: false,
    } as any);
  });

  describe("Files Pagination", () => {
    it("should navigate forward to next page and use correct cursor", async () => {
      const user = userEvent.setup();

      const page1Files = createMockFiles(1, 11);
      const page2Files = createMockFiles(2, 5);

      const useFilesCalls: any[] = [];

      vi.mocked(hooks.useFiles).mockImplementation((params?: any) => {
        useFilesCalls.push(params);

        if (!params || (!params.purpose && !params.limit)) {
          return {
            data: { data: [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (!params.after) {
          return {
            data: { data: page1Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Files[9].id) {
          return {
            data: { data: page2Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useBatches).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(within(container).getByRole("tab", { name: /files/i }));

      await waitFor(() => {
        expect(
          within(container).getByText("file_page1_0.jsonl"),
        ).toBeInTheDocument();
      });

      const activePage = within(container).getByRole("link", {
        current: "page",
      });
      expect(activePage).toHaveTextContent("1");

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      const page2Calls = useFilesCalls.filter(
        (call) => call?.after === page1Files[9].id,
      );
      expect(page2Calls.length).toBeGreaterThan(0);
    });

    it("should navigate backward to previous page using cursor history", async () => {
      const user = userEvent.setup();

      const page1Files = createMockFiles(1, 11);
      const page2Files = createMockFiles(2, 5);

      vi.mocked(hooks.useFiles).mockImplementation((params?: any) => {
        if (!params || (!params.purpose && !params.limit)) {
          return {
            data: { data: [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (!params.after) {
          return {
            data: { data: page1Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Files[9].id) {
          return {
            data: { data: page2Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useBatches).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(within(container).getByRole("tab", { name: /files/i }));

      await waitFor(() => {
        expect(
          within(container).getByText("file_page1_0.jsonl"),
        ).toBeInTheDocument();
      });

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      const prevButton = within(container).getByRole("link", {
        name: /go to previous page/i,
      });
      await user.click(prevButton);

      await waitFor(() => {
        const activePage1 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage1).toHaveTextContent("1");
      });

      expect(prevButton).toHaveClass("pointer-events-none");
    });

    it("should show First button only when on page 2 or higher", async () => {
      const user = userEvent.setup();

      const page1Files = createMockFiles(1, 11);
      const page2Files = createMockFiles(2, 11);
      const page3Files = createMockFiles(3, 5);

      vi.mocked(hooks.useFiles).mockImplementation((params?: any) => {
        if (!params || (!params.purpose && !params.limit)) {
          return {
            data: { data: [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (!params.after) {
          return {
            data: { data: page1Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Files[9].id) {
          return {
            data: { data: page2Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page2Files[9].id) {
          return {
            data: { data: page3Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useBatches).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(within(container).getByRole("tab", { name: /files/i }));

      await waitFor(() => {
        expect(
          within(container).getByText("file_page1_0.jsonl"),
        ).toBeInTheDocument();
      });

      expect(
        within(container).queryByRole("link", { name: /First/i }),
      ).not.toBeInTheDocument();

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      expect(
        within(container).getByRole("link", { name: /First/i }),
      ).toBeInTheDocument();

      const nextButton2 = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton2);

      await waitFor(() => {
        const activePage3 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage3).toHaveTextContent("3");
      });

      expect(
        within(container).getByRole("link", { name: /First/i }),
      ).toBeInTheDocument();
    });

    it("should jump to first page and clear history when First button clicked", async () => {
      const user = userEvent.setup();

      const page1Files = createMockFiles(1, 11);
      const page2Files = createMockFiles(2, 11);
      const page3Files = createMockFiles(3, 5);

      let wasResetToPageOne = false;

      vi.mocked(hooks.useFiles).mockImplementation((params?: any) => {
        if (!params || (!params.purpose && !params.limit)) {
          return {
            data: { data: [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (!params.after && wasResetToPageOne) {
          return {
            data: { data: page1Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (!params.after) {
          return {
            data: { data: page1Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Files[9].id) {
          return {
            data: { data: page2Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page2Files[9].id) {
          wasResetToPageOne = true;
          return {
            data: { data: page3Files },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useBatches).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(within(container).getByRole("tab", { name: /files/i }));

      await waitFor(() => {
        expect(
          within(container).getByText("file_page1_0.jsonl"),
        ).toBeInTheDocument();
      });

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);
      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      const nextButton2 = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton2);
      await waitFor(() => {
        const activePage3 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage3).toHaveTextContent("3");
      });

      const firstButton = within(container).getByRole("link", {
        name: /First/i,
      });
      await user.click(firstButton);

      await waitFor(() => {
        const activePage1 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage1).toHaveTextContent("1");
      });

      expect(
        within(container).queryByRole("link", { name: /First/i }),
      ).not.toBeInTheDocument();
    });

    it("should clear cursor history when page size changes", async () => {
      const user = userEvent.setup();

      const smallPageFiles = createMockFiles(1, 11);
      const largePageFiles = createMockFiles(1, 21);

      vi.mocked(hooks.useFiles).mockImplementation((params?: any) => {
        if (!params || (!params.purpose && !params.limit)) {
          return {
            data: { data: [] },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.limit === 11) {
          return {
            data: { data: smallPageFiles },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.limit === 21) {
          return {
            data: { data: largePageFiles },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useBatches).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(within(container).getByRole("tab", { name: /files/i }));

      await waitFor(() => {
        expect(
          within(container).getByText("file_page1_0.jsonl"),
        ).toBeInTheDocument();
      });

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      const comboboxes = within(container).getAllByRole("combobox");
      const pageSizeSelect = comboboxes.find((el) =>
        el.textContent?.includes("10"),
      )!;
      await user.click(pageSizeSelect);

      await waitFor(() => {
        expect(screen.getByText("20")).toBeInTheDocument();
      });

      const option20 = screen.getByText("20");
      await user.click(option20);

      await waitFor(() => {
        const activePage1 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage1).toHaveTextContent("1");
      });

      const prevButton = within(container).getByRole("link", {
        name: /go to previous page/i,
      });
      expect(prevButton).toHaveClass("pointer-events-none");
    });
  });

  describe("Batches Pagination", () => {
    it("should navigate forward through batch pages", async () => {
      const user = userEvent.setup();

      const page1Batches = createMockBatches(1, 11);
      const page2Batches = createMockBatches(2, 5);

      vi.mocked(hooks.useFiles).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useBatches).mockImplementation((params?: any) => {
        if (!params?.after) {
          return {
            data: { data: page1Batches },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Batches[9].id) {
          return {
            data: { data: page2Batches },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(
        within(container).getByRole("tab", { name: /batches/i }),
      );

      await waitFor(() => {
        const activePage = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage).toHaveTextContent("1");
      });

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });
    });

    it("should navigate backward through batch pages using cursor history", async () => {
      const user = userEvent.setup();

      const page1Batches = createMockBatches(1, 11);
      const page2Batches = createMockBatches(2, 5);

      vi.mocked(hooks.useFiles).mockReturnValue({
        data: { data: [] },
        isLoading: false,
        error: null,
        refetch: vi.fn(),
      } as any);

      vi.mocked(hooks.useBatches).mockImplementation((params?: any) => {
        if (!params?.after) {
          return {
            data: { data: page1Batches },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        if (params.after === page1Batches[9].id) {
          return {
            data: { data: page2Batches },
            isLoading: false,
            error: null,
            refetch: vi.fn(),
          } as any;
        }

        return {
          data: { data: [] },
          isLoading: false,
          error: null,
          refetch: vi.fn(),
        } as any;
      });

      vi.mocked(hooks.useDeleteFile).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      vi.mocked(hooks.useCancelBatch).mockReturnValue({
        mutateAsync: vi.fn(),
        isPending: false,
      } as any);

      const { container } = render(<Batches {...defaultProps} />, {
        wrapper: createWrapper(),
      });

      await user.click(
        within(container).getByRole("tab", { name: /batches/i }),
      );

      await waitFor(() => {
        const activePage = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage).toHaveTextContent("1");
      });

      const nextButton = within(container).getByRole("link", {
        name: /go to next page/i,
      });
      await user.click(nextButton);

      await waitFor(() => {
        const activePage2 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage2).toHaveTextContent("2");
      });

      const prevButton = within(container).getByRole("link", {
        name: /go to previous page/i,
      });
      await user.click(prevButton);

      await waitFor(() => {
        const activePage1 = within(container).getByRole("link", {
          current: "page",
        });
        expect(activePage1).toHaveTextContent("1");
      });

      expect(prevButton).toHaveClass("pointer-events-none");
    });
  });
});
