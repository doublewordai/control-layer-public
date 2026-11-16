import { render, screen, fireEvent, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import {
  vi,
  describe,
  it,
  expect,
  beforeAll,
  afterEach,
  afterAll,
  beforeEach,
} from "vitest";
import React from "react";
import { MemoryRouter } from "react-router-dom";
import { handlers } from "../../../api/control-layer/mocks/handlers";
import { EditEndpointModal } from "./EditEndpointModal";
import type { Endpoint } from "../../../api/control-layer/types";

const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });

  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>{children}</MemoryRouter>
    </QueryClientProvider>
  );
}

const mockEndpoint: Endpoint = {
  id: "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  name: "Test Endpoint",
  description: "Test endpoint description",
  url: "https://api.example.com/v1",
  created_by: "test-user",
  created_at: "2024-01-01T00:00:00Z",
  updated_at: "2024-01-01T00:00:00Z",
  requires_api_key: true,
  model_filter: ["model1", "model2"],
  auth_header_name: "Authorization",
  auth_header_prefix: "Bearer ",
};

describe("EditEndpointModal", () => {
  const mockOnClose = vi.fn();
  const mockOnSuccess = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("does not render when isOpen is false", () => {
    render(
      <EditEndpointModal
        isOpen={false}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );
    expect(screen.queryByText("Edit Endpoint")).not.toBeInTheDocument();
  });

  it("renders modal when isOpen is true", () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    expect(screen.getByText("Edit Endpoint")).toBeInTheDocument();
    expect(screen.getByDisplayValue(mockEndpoint.url)).toBeInTheDocument();
    expect(screen.getByText("Connection")).toBeInTheDocument();
    expect(screen.getByText("Models")).toBeInTheDocument();
  });

  it("initializes form with endpoint data", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    expect(
      screen.getByDisplayValue("https://api.example.com/v1"),
    ).toBeInTheDocument();

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByDisplayValue("Test Endpoint")).toBeInTheDocument();
      expect(
        screen.getByDisplayValue("Test endpoint description"),
      ).toBeInTheDocument();
    });
  });

  it("shows API key field with hint when endpoint requires API key", () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    expect(screen.getByText(/API Key/)).toBeInTheDocument();
    expect(
      screen.getByText("Leave empty to keep existing key"),
    ).toBeInTheDocument();
    expect(screen.getByPlaceholderText("sk-...")).toBeInTheDocument();
  });

  it("shows API key field without hint when endpoint does not require API key", () => {
    const endpointWithoutApiKey = { ...mockEndpoint, requires_api_key: false };

    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={endpointWithoutApiKey}
      />,
      { wrapper: createWrapper() },
    );

    expect(screen.getByText(/API Key/)).toBeInTheDocument();
    expect(screen.getByPlaceholderText("sk-...")).toBeInTheDocument();
    expect(
      screen.queryByText("Leave empty to keep existing key"),
    ).not.toBeInTheDocument();
  });

  it("closes modal when cancel is clicked", () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(mockOnClose).toHaveBeenCalledOnce();
  });

  it("shows Discover Models button on Step 1", () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    expect(
      screen.getByRole("button", { name: /Discover Models/i }),
    ).toBeInTheDocument();
  });

  it("validates URL changes and shows warning", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const urlInput = screen.getByDisplayValue(mockEndpoint.url);
    fireEvent.change(urlInput, { target: { value: "https://new-url.com/v1" } });

    await waitFor(() => {
      expect(
        screen.getByText("(Changed - requires testing)"),
      ).toBeInTheDocument();
    });
  });

  it("shows Discover Models button when URL changes", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const urlInput = screen.getByDisplayValue(mockEndpoint.url);
    fireEvent.change(urlInput, { target: { value: "https://new-url.com/v1" } });

    await waitFor(() => {
      expect(
        screen.getByRole("button", { name: /Discover Models/i }),
      ).toBeInTheDocument();
    });
  });

  it("handles discover models button click", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByText(/Testing Connection.../i)).toBeInTheDocument();
    });
  });

  it("navigates to Step 2 after successful discovery", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByText(/Imported models/i)).toBeInTheDocument();
      expect(
        screen.getByRole("button", { name: /Update Endpoint/i }),
      ).toBeInTheDocument();
    });
  });

  it("shows validation error state on failed model fetch", async () => {
    server.use(
      http.post("/admin/api/v1/endpoints/validate", () => {
        return HttpResponse.json({
          status: "error",
          error: "Connection failed",
        });
      }),
    );

    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getAllByText("Connection failed").length).toBeGreaterThan(
        0,
      );
    });
  });

  it("shows the Add model trigger and imported models on Step 2", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByText(/Imported models/i)).toBeInTheDocument();
      expect(
        screen.getByRole("button", { name: /Add model/i }),
      ).toBeInTheDocument();
    });
  });

  it("opens the add palette and adds a manually-typed model name", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    fireEvent.click(
      screen.getByRole("button", { name: /Discover Models/i }),
    );
    await waitFor(() =>
      expect(screen.getByText(/Imported models/i)).toBeInTheDocument(),
    );

    fireEvent.click(screen.getByRole("button", { name: /Add model/i }));

    const input = await waitFor(() =>
      screen.getByPlaceholderText(/type a (model )?name/i),
    );
    fireEvent.change(input, { target: { value: "my-custom-model" } });

    const manualOption = await waitFor(() =>
      screen.getByText(/Add manually:/i),
    );
    fireEvent.click(manualOption);

    await waitFor(() => {
      expect(screen.getByText("my-custom-model")).toBeInTheDocument();
      expect(screen.getByText(/^new$/i)).toBeInTheDocument();
    });
  });

  it("flags case-only and whitespace-only alias collisions before save (matches backend LOWER(alias))", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    fireEvent.click(screen.getByRole("button", { name: /Discover Models/i }));
    await waitFor(() =>
      expect(screen.getByText(/Imported models/i)).toBeInTheDocument(),
    );

    const addRow = async (name: string) => {
      fireEvent.click(screen.getByRole("button", { name: /Add model/i }));
      const input = await waitFor(() =>
        screen.getByPlaceholderText(/type a (model )?name/i),
      );
      fireEvent.change(input, { target: { value: name } });
      fireEvent.click(await waitFor(() => screen.getByText(/Add manually:/i)));
      await waitFor(() => expect(screen.getByText(name)).toBeInTheDocument());
    };

    await addRow("model-one");
    await addRow("model-two");

    const aliasOne = screen.getByLabelText("Alias for model-one");
    const aliasTwo = screen.getByLabelText("Alias for model-two");

    fireEvent.change(aliasOne, { target: { value: "Test" } });
    fireEvent.change(aliasTwo, { target: { value: "test" } });

    await waitFor(() => {
      expect(
        screen.getByText(/Duplicate aliases detected/i),
      ).toBeInTheDocument();
    });

    fireEvent.change(aliasOne, { target: { value: "  test  " } });

    await waitFor(() => {
      expect(
        screen.getByText(/Duplicate aliases detected/i),
      ).toBeInTheDocument();
    });

    fireEvent.change(aliasOne, { target: { value: "unique-alias" } });

    await waitFor(() => {
      expect(
        screen.queryByText(/Duplicate aliases detected/i),
      ).not.toBeInTheDocument();
    });
  });

  it("requires name field for update", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByDisplayValue("Test Endpoint")).toBeInTheDocument();
    });

    const nameInput = screen.getByDisplayValue("Test Endpoint");
    fireEvent.change(nameInput, { target: { value: "" } });

    const updateButton = screen.getByRole("button", {
      name: /Update Endpoint/i,
    });

    expect(updateButton).toBeDisabled();

    expect(mockOnSuccess).not.toHaveBeenCalled();
  });

  it("requires validation after URL change", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const urlInput = screen.getByDisplayValue(mockEndpoint.url);
    fireEvent.change(urlInput, { target: { value: "https://new-url.com/v1" } });

    await waitFor(() => {
      expect(
        screen.getByText("(Changed - requires testing)"),
      ).toBeInTheDocument();
    });

    expect(mockOnSuccess).not.toHaveBeenCalled();
  });

  it("successfully updates endpoint", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByDisplayValue("Test Endpoint")).toBeInTheDocument();
    });

    const nameInput = screen.getByDisplayValue("Test Endpoint");
    fireEvent.change(nameInput, { target: { value: "Updated Endpoint Name" } });

    const updateButton = screen.getByRole("button", {
      name: /Update Endpoint/i,
    });
    fireEvent.click(updateButton);

    await waitFor(() => {
      expect(mockOnSuccess).toHaveBeenCalledOnce();
      expect(mockOnClose).toHaveBeenCalledOnce();
    });
  });

  it("handles update errors", async () => {
    server.use(
      http.patch("/admin/api/v1/endpoints/*", () => {
        return HttpResponse.json({ error: "Update failed" }, { status: 500 });
      }),
    );

    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByDisplayValue("Test Endpoint")).toBeInTheDocument();
    });

    const updateButton = screen.getByRole("button", {
      name: /Update Endpoint/i,
    });
    fireEvent.click(updateButton);

    await waitFor(() => {
      expect(
        screen.getByText(/Failed to update endpoint/i),
      ).toBeInTheDocument();
    });

    expect(mockOnSuccess).not.toHaveBeenCalled();
  });

  it("handles discover models flow", async () => {
    render(
      <EditEndpointModal
        isOpen={true}
        onClose={mockOnClose}
        onSuccess={mockOnSuccess}
        endpoint={mockEndpoint}
      />,
      { wrapper: createWrapper() },
    );

    const urlInput = screen.getByDisplayValue(mockEndpoint.url);
    fireEvent.change(urlInput, { target: { value: "https://new-url.com/v1" } });

    await waitFor(() => {
      expect(
        screen.getByText("(Changed - requires testing)"),
      ).toBeInTheDocument();
    });

    const discoverButton = screen.getByRole("button", {
      name: /Discover Models/i,
    });
    fireEvent.click(discoverButton);

    await waitFor(() => {
      expect(screen.getByText(/Testing Connection.../i)).toBeInTheDocument();
    });
  });
  it.each([false, true])("submits server kind only when changed: %s", async (changeKind) => {
    let submitted: Record<string, unknown> | undefined;
    server.use(http.patch("/admin/api/v1/endpoints/:id", async ({ request }) => {
      submitted = await request.json() as Record<string, unknown>;
      return HttpResponse.json({ ...mockEndpoint, ...submitted });
    }));
    render(<EditEndpointModal isOpen onClose={mockOnClose} onSuccess={mockOnSuccess} endpoint={{ ...mockEndpoint, kind: "hosted" }} />, { wrapper: createWrapper() });
    if (changeKind) {
      fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: /advanced configuration/i }));
      fireEvent.change(screen.getByLabelText("Server kind"), { target: { value: "dynamo" } });
      fireEvent.keyDown(screen.getByLabelText("Server kind"), { key: "Escape" });
    }
    fireEvent.click(within(screen.getByRole("dialog")).getByRole("button", { name: /discover models/i }));
    const update = await screen.findByRole("button", { name: /update endpoint/i });
    fireEvent.click(update);
    await waitFor(() => expect(submitted).toBeDefined());
    if (changeKind) expect(submitted?.kind).toBe("dynamo");
    else expect(submitted).not.toHaveProperty("kind");
  });

});
