import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { CreateBatchModal } from "./CreateBatchModal";
import * as hooks from "../../../api/control-layer/hooks";
import * as contexts from "../../../contexts";

vi.mock("../../../api/control-layer/hooks", () => ({
  useCreateBatch: vi.fn(),
  useUploadFile: vi.fn(),
  useUploadFileWithProgress: vi.fn(),
  useFiles: vi.fn(),
  useFileCostEstimate: vi.fn(),
  useApiKeys: vi.fn(),
  useUser: vi.fn(),
  useConfig: vi.fn(() => ({
    data: {
      docs_url: "https://docs.example.com",
      docs_jsonl_url: "https://docs.example.com/jsonl",
    },
  })),
}));

vi.mock("../../../contexts", () => ({
  useOrganizationContext: vi.fn(),
}));

vi.mock("sonner", () => ({
  toast: {
    success: vi.fn(),
    error: vi.fn(),
  },
}));

const mockFile = {
  id: "file-123",
  object: "file" as const,
  bytes: 1024000,
  created_at: 1730995200,
  expires_at: 1765065600,
  filename: "test-batch.jsonl",
  purpose: "batch" as const,
};

const CURRENT_USER_ID = "user-1";

const mockApiKeys = [
  {
    id: "key-1",
    name: "My realtime key",
    purpose: "realtime",
    created_at: "2026-01-01T00:00:00Z",
    created_by: CURRENT_USER_ID,
    spend_limit: null,
    spend: null,
  },
  {
    id: "key-2",
    name: "Capped key",
    purpose: "realtime",
    created_at: "2026-01-01T00:00:00Z",
    created_by: CURRENT_USER_ID,
    spend_limit: "10",
    spend: "3.2",
  },
  {
    id: "key-3",
    name: "Other member key",
    purpose: "realtime",
    created_at: "2026-01-01T00:00:00Z",
    created_by: "user-2",
    spend_limit: null,
    spend: null,
  },
];

const personalContext = {
  activeOrganizationId: null,
  activeOrganization: null,
  isOrgContext: false,
  setActiveOrganization: vi.fn(),
};

const orgContext = (role: string, canManageKeys = true) => ({
  activeOrganizationId: "org-1",
  activeOrganization: {
    id: "org-1",
    name: "Test Org",
    role,
    zero_data_retention: false,
    can_manage_keys: canManageKeys,
  },
  isOrgContext: true,
  setActiveOrganization: vi.fn(),
});

const mockCreateBatch = (isPending = false) => {
  const mutateAsync = vi.fn().mockResolvedValue({});
  vi.mocked(hooks.useCreateBatch).mockReturnValue({
    mutateAsync,
    isPending,
    isError: false,
    error: null,
    isSuccess: false,
    data: undefined,
    mutate: vi.fn(),
    reset: vi.fn(),
    status: isPending ? "pending" : "idle",
    context: undefined,
    failureCount: 0,
    failureReason: null,
    isIdle: !isPending,
    isPaused: false,
    submittedAt: 0,
    variables: undefined,
  } as any);
  return mutateAsync;
};

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, staleTime: 0 },
      mutations: { retry: false },
    },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe("CreateBatchModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    vi.mocked(hooks.useUploadFile).mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
      isError: false,
      error: null,
      isSuccess: false,
      data: undefined,
      mutate: vi.fn(),
      reset: vi.fn(),
      status: "idle",
      context: undefined,
      failureCount: 0,
      failureReason: null,
      isIdle: true,
      isPaused: false,
      submittedAt: 0,
      variables: undefined,
    } as any);

    vi.mocked(hooks.useUploadFileWithProgress).mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
      isError: false,
      error: null,
      isSuccess: false,
      data: undefined,
      mutate: vi.fn(),
      reset: vi.fn(),
      status: "idle",
      context: undefined,
      failureCount: 0,
      failureReason: null,
      isIdle: true,
      isPaused: false,
      submittedAt: 0,
      variables: undefined,
    } as any);

    vi.mocked(hooks.useFiles).mockReturnValue({
      data: { data: [] },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    } as any);

    vi.mocked(hooks.useConfig).mockReturnValue({
      data: {
        batches: {
          allowed_completion_windows: ["24h"],
        },
      },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    } as any);

    vi.mocked(hooks.useFileCostEstimate).mockReturnValue({
      data: null,
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    } as any);

    vi.mocked(contexts.useOrganizationContext).mockReturnValue(personalContext);

    vi.mocked(hooks.useUser).mockReturnValue({
      data: { id: CURRENT_USER_ID },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    } as any);

    vi.mocked(hooks.useApiKeys).mockReturnValue({
      data: { data: mockApiKeys, total_count: mockApiKeys.length },
      isLoading: false,
      error: null,
      refetch: vi.fn(),
    } as any);
  });

  describe("Basic interactions", () => {
    it("should close modal when Cancel button is clicked", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      await user.click(cancelButton);

      expect(onClose).toHaveBeenCalled();
      expect(mutateAsync).not.toHaveBeenCalled();
    });

    it("should submit when Create Batch button is clicked", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const onSuccess = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          onSuccess={onSuccess}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const descriptionInput =
        screen.getByPlaceholderText(/Data generation task/i);
      await user.type(descriptionInput, "Test batch");

      const createButton = screen.getByRole("button", {
        name: /create batch/i,
      });
      await user.click(createButton);

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: {
            batch_description: "Test batch",
          },
        });
      });

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalled();
        expect(onClose).toHaveBeenCalled();
      });
    });

    it("should disable Create Batch button when no file is selected", async () => {
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={undefined}
        />,
        { wrapper: createWrapper() },
      );

      const createButton = screen.getByRole("button", {
        name: /create batch/i,
      });

      expect(createButton).toBeDisabled();
    });

    it("should disable buttons when mutation is pending", async () => {
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: true,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "pending",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: false,
        isPaused: false,
        submittedAt: Date.now(),
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const cancelButton = screen.getByRole("button", { name: /cancel/i });
      const createButton = screen.getByRole("button", { name: /creating/i });

      expect(cancelButton).toBeDisabled();
      expect(createButton).toBeDisabled();
    });
  });

  describe("Enter key submission", () => {
    it("should submit the form when Enter is pressed in description field", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const onSuccess = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          onSuccess={onSuccess}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const descriptionInput =
        screen.getByPlaceholderText(/Data generation task/i);
      await user.click(descriptionInput);
      await user.type(descriptionInput, "Test batch description");

      await user.keyboard("{Enter}");

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: {
            batch_description: "Test batch description",
          },
        });
      });

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalled();
        expect(onClose).toHaveBeenCalled();
      });
    });

    it("should not submit when Enter is pressed if no file is selected", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          preselectedFile={undefined}
        />,
        { wrapper: createWrapper() },
      );

      const descriptionInput =
        screen.getByPlaceholderText(/Data generation task/i);
      await user.click(descriptionInput);
      await user.type(descriptionInput, "Test description");

      await user.keyboard("{Enter}");

      expect(mutateAsync).not.toHaveBeenCalled();
    });

    it("should not submit when Enter is pressed if mutation is pending", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: true,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "pending",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: false,
        isPaused: false,
        submittedAt: Date.now(),
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const descriptionInput =
        screen.getByPlaceholderText(/Data generation task/i);
      await user.click(descriptionInput);
      await user.type(descriptionInput, "Test description");

      await user.keyboard("{Enter}");

      expect(mutateAsync).not.toHaveBeenCalled();
    });
  });

  describe("API key selector", () => {
    it("should not render the selector in personal context", () => {
      mockCreateBatch();

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      expect(
        screen.queryByRole("combobox", { name: /bill to api key/i }),
      ).not.toBeInTheDocument();
    });

    it("should show the selector with the user's org keys in org context", async () => {
      const user = userEvent.setup();
      mockCreateBatch();
      vi.mocked(contexts.useOrganizationContext).mockReturnValue(
        orgContext("member"),
      );

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      expect(vi.mocked(hooks.useApiKeys)).toHaveBeenCalledWith(
        "org-1",
        expect.any(Object),
      );

      const trigger = screen.getByRole("combobox", {
        name: /bill to api key/i,
      });
      await user.click(trigger);

      expect(
        screen.getByRole("option", { name: /account default/i }),
      ).toBeInTheDocument();
      expect(
        screen.getByRole("option", { name: /my realtime key/i }),
      ).toBeInTheDocument();

      expect(
        screen.getByRole("option", { name: /capped key.*\$3\.20 of \$10\.00 used/i }),
      ).toBeInTheDocument();

      expect(
        screen.queryByRole("option", { name: /other member key/i }),
      ).not.toBeInTheDocument();
    });

    it("offers admins only their OWN keys — managing others' keys never means billing to them", async () => {
      const user = userEvent.setup();
      mockCreateBatch();
      vi.mocked(contexts.useOrganizationContext).mockReturnValue(
        orgContext("admin"),
      );

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      await user.click(
        screen.getByRole("combobox", { name: /bill to api key/i }),
      );

      expect(
        screen.queryByRole("option", { name: /other member key/i }),
      ).not.toBeInTheDocument();
    });

    it("should require a key selection for members without key management", async () => {
      const user = userEvent.setup();
      const mutateAsync = mockCreateBatch();
      vi.mocked(contexts.useOrganizationContext).mockReturnValue(
        orgContext("member", false),
      );

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      expect(
        screen.getByText(
          /your organization requires selecting one of your issued api keys/i,
        ),
      ).toBeInTheDocument();
      const createButton = screen.getByRole("button", {
        name: /create batch/i,
      });
      expect(createButton).toBeDisabled();

      await user.click(
        screen.getByRole("combobox", { name: /bill to api key/i }),
      );

      expect(
        screen.queryByRole("option", { name: /account default/i }),
      ).not.toBeInTheDocument();

      await user.click(
        screen.getByRole("option", { name: /my realtime key/i }),
      );

      expect(createButton).toBeEnabled();
      await user.click(createButton);

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: undefined,
          api_key_id: "key-1",
        });
      });
    });

    it("should include the selected key id in the create payload", async () => {
      const user = userEvent.setup();
      const mutateAsync = mockCreateBatch();
      vi.mocked(contexts.useOrganizationContext).mockReturnValue(
        orgContext("member"),
      );

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      await user.click(
        screen.getByRole("combobox", { name: /bill to api key/i }),
      );
      await user.click(screen.getByRole("option", { name: /capped key/i }));

      await user.click(screen.getByRole("button", { name: /create batch/i }));

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: undefined,
          api_key_id: "key-2",
        });
      });
    });

    it("should omit api_key_id when the account default is kept in org context", async () => {
      const user = userEvent.setup();
      const mutateAsync = mockCreateBatch();
      vi.mocked(contexts.useOrganizationContext).mockReturnValue(
        orgContext("member"),
      );

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={vi.fn()}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      await user.click(screen.getByRole("button", { name: /create batch/i }));

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: undefined,
        });
      });
    });

    it("should submit with empty description when Enter is pressed", async () => {
      const user = userEvent.setup();
      const onClose = vi.fn();
      const onSuccess = vi.fn();
      const mutateAsync = vi.fn().mockResolvedValue({});

      vi.mocked(hooks.useCreateBatch).mockReturnValue({
        mutateAsync,
        isPending: false,
        isError: false,
        error: null,
        isSuccess: false,
        data: undefined,
        mutate: vi.fn(),
        reset: vi.fn(),
        status: "idle",
        context: undefined,
        failureCount: 0,
        failureReason: null,
        isIdle: true,
        isPaused: false,
        submittedAt: 0,
        variables: undefined,
      } as any);

      render(
        <CreateBatchModal
          isOpen={true}
          onClose={onClose}
          onSuccess={onSuccess}
          preselectedFile={mockFile}
        />,
        { wrapper: createWrapper() },
      );

      const descriptionInput =
        screen.getByPlaceholderText(/Data generation task/i);
      await user.click(descriptionInput);

      await user.keyboard("{Enter}");

      await waitFor(() => {
        expect(mutateAsync).toHaveBeenCalledWith({
          input_file_id: "file-123",
          endpoint: "/v1/chat/completions",
          completion_window: "24h",
          metadata: undefined,
        });
      });

      await waitFor(() => {
        expect(onSuccess).toHaveBeenCalled();
        expect(onClose).toHaveBeenCalled();
      });
    });
  });
});
