import { render, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setupServer } from "msw/node";
import type { ReactNode } from "react";
import {
  describe,
  it,
  expect,
  beforeAll,
  afterEach,
  afterAll,
  vi,
} from "vitest";
import Playground from "./Playground";
import { handlers } from "../../../../api/control-layer/mocks/handlers";

const server = setupServer(...handlers);

beforeAll(() => {
  server.listen({ onUnhandledRequest: "error" });
  Element.prototype.scrollIntoView = vi.fn();
});
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const { mockOpenAI } = vi.hoisted(() => {
  const mockOpenAI = {
    chat: {
      completions: {
        create: vi.fn().mockResolvedValue({
          choices: [
            {
              delta: { content: "Hello! How can I help you today?" },
            },
          ],
          async *[Symbol.asyncIterator]() {
            yield { choices: [{ delta: { content: "Hello! " } }] };
            yield { choices: [{ delta: { content: "How can I " } }] };
            yield { choices: [{ delta: { content: "help you today?" } }] };
          },
        }),
      },
    },
    embeddings: {
      create: vi
        .fn()
        .mockResolvedValueOnce({
          data: [
            {
              embedding: [0.8, 0.6, 0.7, 0.5, 0.9],
            },
          ],
        })
        .mockResolvedValueOnce({
          data: [
            {
              embedding: [0.7, 0.5, 0.8, 0.6, 0.8],
            },
          ],
        }),
    },
  };
  return { mockOpenAI };
});

vi.mock("openai", () => ({
  default: vi.fn(function () {
    return mockOpenAI;
  }),
}));

let queryClient: QueryClient;

function createWrapper(initialEntries = ["/"]) {
  queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });

  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={initialEntries}>{children}</MemoryRouter>
    </QueryClientProvider>
  );
}

describe("Playground Component - Functional Tests", () => {
  afterEach(() => {
    if (queryClient) {
      queryClient.clear();
      queryClient.cancelQueries();
    }
  });
  it("loads playground page and shows welcome state", async () => {
    const { container } = render(<Playground />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(
        within(container).getByRole("main", { name: /welcome to playground/i }),
      ).toBeInTheDocument();
    });

    expect(
      within(container).getByRole("heading", {
        name: /welcome to the playground/i,
      }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("combobox", { name: /select model/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("button", { name: /back to models/i }),
    ).toBeInTheDocument();
  });

  it("enables model selector when models load", async () => {
    const { container } = render(<Playground />, { wrapper: createWrapper() });

    const modelSelect = within(container).getByRole("combobox", {
      name: /select model/i,
    });

    expect(modelSelect).toHaveAttribute("aria-expanded", "false");
    expect(modelSelect).toHaveAttribute("role", "combobox");
    expect(modelSelect.textContent).toMatch(/search models|Qwen/i);
  });

  it("shows model selector ready for available models", async () => {
    const { container } = render(<Playground />, { wrapper: createWrapper() });

    const modelSelect = within(container).getByRole("combobox", {
      name: /select model/i,
    });

    expect(modelSelect).toHaveAttribute("aria-label", "Select model");
    expect(modelSelect).toHaveAttribute("aria-expanded", "false");

    expect(modelSelect).not.toHaveTextContent(/loading/i);
    expect(modelSelect).not.toHaveTextContent(/no models/i);
    expect(modelSelect.textContent).toMatch(/search models|Qwen/i);
  });

  it("shows no error messages when models load successfully", async () => {
    const { container } = render(<Playground />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(
        within(container).getByRole("main", { name: /welcome to playground/i }),
      ).toBeInTheDocument();
    });

    expect(
      within(container).queryByText(/failed to load models/i),
    ).not.toBeInTheDocument();
  });

  it("displays essential elements on mobile viewport", async () => {
    Object.defineProperty(window, "innerWidth", {
      value: 375,
      configurable: true,
    });

    const { container } = render(<Playground />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(
        within(container).getByRole("main", { name: /welcome to playground/i }),
      ).toBeInTheDocument();
    });

    expect(
      within(container).getByRole("button", { name: /back to models/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("combobox", { name: /select model/i }),
    ).toBeInTheDocument();
  });

  it("loads chat playground when model parameter is provided", async () => {
    const { container } = render(<Playground />, {
      wrapper: createWrapper(["/?model=d1a2b3c4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"]),
    });

    await waitFor(() => {
      const modelSelects = within(container).getAllByRole("combobox", {
        name: /select model/i,
      });
      expect(modelSelects[0]).toHaveTextContent("Qwen/Qwen3.5-397B-A17B-FP8");
    });

    expect(
      within(container).queryByRole("main", { name: /welcome to playground/i }),
    ).not.toBeInTheDocument();

    expect(
      within(container).getByRole("heading", { name: /chat playground/i }),
    ).toBeInTheDocument();

    expect(
      within(container).getByRole("textbox", { name: /message input/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("button", { name: /send message/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("button", { name: /clear conversation/i }),
    ).toBeInTheDocument();

    expect(
      within(container).getByRole("status", { name: /empty conversation/i }),
    ).toBeInTheDocument();
  });

  it("sends message and displays conversation", async () => {
    const user = userEvent.setup();
    const { container } = render(<Playground />, {
      wrapper: createWrapper(["/?model=d1a2b3c4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"]),
    });

    await waitFor(() => {
      expect(
        within(container).getByRole("textbox", { name: /message input/i }),
      ).toBeInTheDocument();
    });

    const messageInput = within(container).getByRole("textbox", {
      name: /message input/i,
    });
    await user.type(messageInput, "Hello!");
    await user.click(
      within(container).getByRole("button", { name: /send message/i }),
    );

    expect(within(container).getByText("Hello!")).toBeInTheDocument();

    await waitFor(() => {
      expect(
        within(container).getByText("Hello! How can I help you today?"),
      ).toBeInTheDocument();
    });

    expect(
      within(container).queryByRole("status", { name: /empty conversation/i }),
    ).not.toBeInTheDocument();

    expect(messageInput).toHaveValue("");
  });

  it("loads embedding playground and compares text similarity", async () => {
    const user = userEvent.setup();
    const { container } = render(<Playground />, {
      wrapper: createWrapper(["/?model=d7a8b9c0-e1f2-4a3b-4c5d-6e7f8a9b0c1d"]),
    });

    await waitFor(() => {
      expect(
        within(container).getByRole("heading", {
          name: /embeddings playground/i,
        }),
      ).toBeInTheDocument();
    });

    expect(
      within(container).getByRole("textbox", { name: /text a input/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("textbox", { name: /text b input/i }),
    ).toBeInTheDocument();
    expect(
      within(container).getByRole("button", { name: /compare similarity/i }),
    ).toBeInTheDocument();

    const firstTextInput = within(container).getByRole("textbox", {
      name: /text a input/i,
    });
    const secondTextInput = within(container).getByRole("textbox", {
      name: /text b input/i,
    });

    await user.type(firstTextInput, "The cat sat on the mat");
    await user.type(secondTextInput, "A feline rested on the rug");

    await user.click(
      within(container).getByRole("button", { name: /compare similarity/i }),
    );

    await waitFor(() => {
      expect(
        within(container).getByRole("heading", { name: /similarity results/i }),
      ).toBeInTheDocument();
      expect(
        within(container).getByRole("status", { name: /similarity category/i }),
      ).toHaveTextContent(/very similar/i);
    });
  });
});
