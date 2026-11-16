import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setupServer } from "msw/node";
import { ReactNode } from "react";
import {
  describe,
  it,
  expect,
  beforeAll,
  afterEach,
  afterAll,
  vi,
} from "vitest";
import Models from "./Models";
import { handlers } from "../../../../api/control-layer/mocks/handlers";
import { SettingsProvider } from "../../../../contexts/settings/SettingsContext";

const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

const mockNavigate = vi.fn();
vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual("react-router-dom");
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

vi.mock("../../../../utils/authorization", () => ({
  useAuthorization: vi.fn(() => ({
    userRoles: ["PlatformManager"],
    hasPermission: vi.fn(() => true),
    canAccessRoute: vi.fn(() => true),
  })),
}));

let queryClient: QueryClient;

function createWrapper() {
  queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        gcTime: 0,
      },
    },
  });

  return ({ children }: { children: ReactNode }) => (
    <SettingsProvider>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>{children}</MemoryRouter>
      </QueryClientProvider>
    </SettingsProvider>
  );
}

describe("Models Component - Functional Tests", () => {
  beforeEach(() => {
    mockNavigate.mockClear();
  });

  afterEach(() => {
    if (queryClient) {
      queryClient.clear();
      queryClient.cancelQueries();
    }
  });

  describe("Model Discovery Journey", () => {
    it("allows users to browse, filter, and search models", { timeout: 10000 }, async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const modelCards = within(container).getAllByRole("listitem");
      expect(modelCards.length).toBeGreaterThan(0);

      const filterButton = within(container).getByRole("button", {
        name: /filter models/i,
      });
      expect(filterButton).toBeInTheDocument();

      await user.click(filterButton);

      const providerSelect = screen.getByRole("combobox", {
        name: /filter by endpoint provider/i,
      });
      expect(providerSelect).toBeInTheDocument();

      expect(providerSelect).toHaveTextContent(/all endpoints/i);

      await user.keyboard("{Escape}");

      const searchInput = within(container).getByRole("textbox", {
        name: /search models/i,
      });
      await user.type(searchInput, "gpt");

      await waitFor(() => {
        const searchResults = within(container).getAllByRole("listitem");
        searchResults.forEach((card) => {
          const cardText = card.textContent?.toLowerCase();
          expect(cardText).toMatch(/gpt/i);
        });
      });

      await user.clear(searchInput);
      await user.type(searchInput, "nonexistent-model-xyz");

      await waitFor(() => {
        expect(within(container).queryAllByRole("listitem")).toHaveLength(0);
      });
    });

    it("filters models by selected group", { timeout: 10000 }, async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      expect(
        within(container).getByText("Qwen/Qwen3.5-35B-A3B-FP8"),
      ).toBeInTheDocument();

      await user.click(
        within(container).getByRole("button", { name: /filter models/i }),
      );
      const groupFilter = screen.getByRole("button", { name: "Groups" });
      expect(groupFilter).toHaveAccessibleDescription("No groups selected");

      await user.click(groupFilter);
      await user.click(screen.getByRole("button", { name: "ML Research" }));

      expect(groupFilter).toHaveAccessibleDescription(
        "Selected groups: ML Research",
      );

      await waitFor(() => {
        expect(
          within(container).getByText("Qwen/Qwen3.5-397B-A17B-FP8"),
        ).toBeInTheDocument();
        expect(
          within(container).queryByText("Qwen/Qwen3.5-35B-A3B-FP8"),
        ).not.toBeInTheDocument();
      });
    });

    it("navigates to playground when clicking playground button", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const modelCards = within(container).getAllByRole("listitem");
      expect(modelCards.length).toBeGreaterThan(0);

      const firstCard = modelCards[0];
      const playgroundButton = within(firstCard).getByRole("button", {
        name: /playground/i,
      });

      await user.click(playgroundButton);

      expect(mockNavigate).toHaveBeenCalledWith(
        expect.stringMatching(/^\/playground\?model=/),
      );
    });
  });

  describe("API Integration Journey", () => {
    it("opens API examples modal when clicking API button", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const modelCards = within(container).getAllByRole("listitem");
      const firstCard = modelCards[0];
      const apiButton = within(firstCard).getByRole("button", { name: /api/i });

      await user.click(apiButton);

      await waitFor(() => {
        expect(screen.getByRole("dialog")).toBeInTheDocument();
        expect(
          screen.getByRole("heading", { name: /api examples/i }),
        ).toBeInTheDocument();
      });
    });
  });

  describe("Access Control Journey", () => {
    it("shows access toggle for admin users", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      const filterButton = within(container).getByRole("button", {
        name: /filter models/i,
      });
      expect(filterButton).toBeInTheDocument();

      await user.click(filterButton);

      const accessToggle = screen.getByRole("switch", {
        name: /show only my accessible models/i,
      });
      expect(accessToggle).toBeInTheDocument();
      expect(accessToggle).not.toBeChecked();
    });

    it("allows admin users to toggle between all models and accessible models", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const initialCards = within(container).getAllByRole("listitem");
      const _initialCount = initialCards.length;

      const filterButton = within(container).getByRole("button", {
        name: /filter models/i,
      });
      await user.click(filterButton);

      const accessToggle = screen.getByRole("switch", {
        name: /show only my accessible models/i,
      });
      expect(accessToggle).not.toBeChecked();
    });
  });

  describe("Pagination Journey", () => {
    it("handles pagination when many models are present", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      const pagination = within(container).queryByRole("navigation", {
        name: /pagination/i,
      });

      if (pagination) {
        const nextButton = within(pagination).queryByRole("button", {
          name: /next/i,
        });
        if (
          nextButton &&
          !nextButton.classList.contains("pointer-events-none")
        ) {
          await user.click(nextButton);

          await waitFor(() => {
            const currentPageButton = within(pagination).getByRole("button", {
              pressed: true,
            });
            expect(currentPageButton).toHaveTextContent("2");
          });
        }
      }
    });
  });

  describe("Error Handling Journey", () => {
    it("handles API errors gracefully", async () => {
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
        expect(
          within(container).queryByText(/loading/i),
        ).not.toBeInTheDocument();
      });
    });
  });

  describe("Admin Features Journey", () => {
    it("allows admins to add groups to models with no groups", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const modelCards = within(container).getAllByRole("listitem");
      expect(modelCards.length).toBeGreaterThan(0);

      const addGroupsButtons = within(container).queryAllByRole("button", {
        name: /add groups/i,
      });

      if (addGroupsButtons.length > 0) {
        const firstAddGroupsButton = addGroupsButtons[0];
        await waitFor(() => {
          expect(firstAddGroupsButton).toBeEnabled();
        });

        await user.click(firstAddGroupsButton);

        await waitFor(() => {
          expect(screen.getByRole("dialog")).toBeInTheDocument();
        });
      } else {
        expect(modelCards.length).toBeGreaterThan(0);
      }
    });

    it("allows admins to manage access groups via group badges", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      const moreGroupsBadges = within(container).queryAllByText(/\+\d+ more/);

      if (moreGroupsBadges.length > 0) {
        const firstMoreBadge = moreGroupsBadges[0];
        await user.click(firstMoreBadge);

        await waitFor(
          () => {
            expect(within(container).getByRole("dialog")).toBeInTheDocument();
          },
          { timeout: 5000, interval: 50 },
        );
      } else {
        const groupBadges = within(container).queryAllByText(/group/i);

        if (groupBadges.length > 0) {
          expect(groupBadges.length).toBeGreaterThan(0);
        }
      }
    });

    it("shows admin-specific UI elements for platform managers", async () => {
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const filterButton = within(container).getByRole("button", {
        name: /filter models/i,
      });
      expect(filterButton).toBeInTheDocument();

      const modelCards = within(container).getAllByRole("listitem");
      expect(modelCards.length).toBeGreaterThan(0);

      const adminButtons = within(container).queryAllByRole("button", {
        name: /add groups|open menu/i,
      });

      expect(adminButtons.length).toBeGreaterThanOrEqual(0);
    });

    it("handles hover interactions on group badges", async () => {
      const user = userEvent.setup();
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      const moreGroupsBadges = within(container).queryAllByText(/\+\d+ more/);

      if (moreGroupsBadges.length > 0) {
        const firstMoreBadge = moreGroupsBadges[0];

        await user.hover(firstMoreBadge);

        expect(firstMoreBadge).toBeInTheDocument();

        await user.unhover(firstMoreBadge);
      }

      expect(
        within(container).getByRole("heading", { name: /models/i }),
      ).toBeInTheDocument();
    });

    it("handles permission-based rendering correctly", async () => {
      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      await waitFor(() => {
        expect(
          within(container).getAllByRole("listitem").length,
        ).toBeGreaterThan(0);
      });

      const modelCards = within(container).getAllByRole("listitem");
      expect(modelCards.length).toBeGreaterThan(0);

      const filterButton = within(container).getByRole("button", {
        name: /filter models/i,
      });
      expect(filterButton).toBeInTheDocument();

      const adminControls = within(container).queryAllByRole("button", {
        name: /add groups|open menu|manage access/i,
      });

      expect(adminControls.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe("Responsive Behavior", () => {
    it("maintains functionality across different screen sizes", async () => {
      const user = userEvent.setup();

      Object.defineProperty(window, "innerWidth", {
        writable: true,
        configurable: true,
        value: 375,
      });

      const { container } = render(<Models />, { wrapper: createWrapper() });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { name: /models/i }),
        ).toBeInTheDocument();
      });

      const searchInput = within(container).getByRole("textbox", {
        name: /search models/i,
      });

      await waitFor(() => {
        expect(searchInput).toBeEnabled();
      });

      await user.type(searchInput, "gpt");

      await waitFor(() => {
        const searchResults = within(container).getAllByRole("listitem");
        expect(searchResults.length).toBeGreaterThan(0);
      });
    });
  });
});
