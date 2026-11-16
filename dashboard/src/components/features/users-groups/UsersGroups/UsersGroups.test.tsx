import { render, within, waitFor, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { setupServer } from "msw/node";
import { http, HttpResponse } from "msw";
import React, { ReactNode } from "react";
import { describe, it, expect, beforeAll, afterEach, afterAll } from "vitest";
import userEvent from "@testing-library/user-event";
import UsersGroups from "./UsersGroups";
import { handlers } from "../../../../api/control-layer/mocks/handlers";
import { SettingsProvider } from "../../../../contexts";

const server = setupServer(...handlers);

beforeAll(() => server.listen({ onUnhandledRequest: "error" }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
        staleTime: 0,
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

describe("UsersGroups Component", () => {
  it("renders without crashing", async () => {
    const { container } = render(<UsersGroups />, { wrapper: createWrapper() });

    expect(
      within(container).getByText("Loading users and groups..."),
    ).toBeInTheDocument();

    await waitFor(() => {
      expect(within(container).getByText("User Access")).toBeInTheDocument();
    });
  });

  it("renders users data when loaded", async () => {
    const { container } = render(<UsersGroups />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(within(container).getByText("Sarah Chen")).toBeInTheDocument();
      expect(
        within(container).getByText("sarah.chen@acme.com"),
      ).toBeInTheDocument();
    });
  });

  it("renders error state when API fails", async () => {
    server.use(
      http.get("/admin/api/v1/users", () => {
        return HttpResponse.json(
          { error: "Failed to fetch users" },
          { status: 500 },
        );
      }),
    );

    const { container } = render(<UsersGroups />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(within(container).getByText(/Error:/)).toBeInTheDocument();
    });
  });

  it("renders empty state when no data exists", async () => {
    server.use(
      http.get("/admin/api/v1/users", () => {
        return HttpResponse.json({
          data: [],
          total_count: 0,
          skip: 0,
          limit: 10,
        });
      }),
      http.get("/admin/api/v1/groups", () => {
        return HttpResponse.json({
          data: [],
          total_count: 0,
          skip: 0,
          limit: 10,
        });
      }),
    );

    const { container } = render(<UsersGroups />, { wrapper: createWrapper() });

    await waitFor(() => {
      expect(
        within(container).getByRole("heading", { level: 1 }),
      ).toBeInTheDocument();
      expect(
        within(container).getByRole("tab", { name: "Users" }),
      ).toBeInTheDocument();
    });
  });

  describe("User Management Journey", () => {
    it("searches users without offering account creation", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { level: 1 }),
        ).toBeInTheDocument();
        expect(within(container).getByRole("table")).toBeInTheDocument();
        expect(within(container).getByText("Sarah Chen")).toBeInTheDocument();
      });

      const usersTab = within(container).getByRole("tab", { name: "Users" });
      expect(usersTab).toHaveAttribute("aria-selected", "true");

      expect(within(container).getByText("James Wilson")).toBeInTheDocument();
      expect(within(container).getByText("Alex Rodriguez")).toBeInTheDocument();

      const searchInput = within(container).getByRole("textbox", {
        name: /search users/i,
      });
      await user.type(searchInput, "Sarah");
      expect(searchInput).toHaveValue("Sarah");

      await waitFor(
        () => {
          expect(
            within(container).queryByText("James Wilson"),
          ).not.toBeInTheDocument();
        },
        { timeout: 3000, interval: 100 },
      );

      await waitFor(() => {
        expect(within(container).getByText("Sarah Chen")).toBeInTheDocument();
      });

      const searchInputAfterSearch = within(container).getByRole("textbox", {
        name: /search users/i,
      });
      await user.clear(searchInputAfterSearch);

      await waitFor(
        () => {
          expect(
            within(container).getByText("James Wilson"),
          ).toBeInTheDocument();
          expect(within(container).getByText("Sarah Chen")).toBeInTheDocument();
        },
        { timeout: 3000, interval: 100 },
      );

      expect(within(container).queryByRole("button", { name: /add user/i })).not.toBeInTheDocument();
    });

    it("offers ZDR support without profile, role, or deletion controls", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, { wrapper: createWrapper() });
      await within(container).findByText("Sarah Chen");
      expect(within(container).queryByTitle("Delete user")).not.toBeInTheDocument();
      await user.click(within(container).getAllByTitle("Edit user")[0]);
      const dialog = await screen.findByRole("dialog", { name: /edit user/i });
      expect(within(dialog).queryByRole("textbox", { name: /display name/i })).not.toBeInTheDocument();
      expect(within(dialog).queryByRole("checkbox", { name: /standard user/i })).not.toBeInTheDocument();
      expect(within(dialog).getByRole("switch", { name: /zero data retention/i })).toBeInTheDocument();
      await user.click(within(dialog).getByRole("button", { name: /cancel/i }));
      await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    });

    it("tests Edit Group Modal through group dropdown", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { level: 1 }),
        ).toBeInTheDocument();
      });

      const groupsTab = within(container).getByRole("tab", { name: "Groups" });
      await user.click(groupsTab);
      expect(groupsTab).toHaveAttribute("aria-selected", "true");

      await waitFor(() => {
        expect(within(container).getByText("Engineering")).toBeInTheDocument();
      });

      const groupActionMenus = within(container).getAllByRole("button", {
        name: /open menu/i,
      });
      expect(groupActionMenus.length).toBeGreaterThan(0);

      await user.click(groupActionMenus[0]);

      await waitFor(() => {
        expect(screen.getByRole("menu")).toBeInTheDocument();
        expect(
          screen.getByRole("menuitem", { name: "Edit Group" }),
        ).toBeInTheDocument();
      });

      const editGroupButton = screen.getByRole("menuitem", {
        name: "Edit Group",
      });
      await user.click(editGroupButton);

      await waitFor(() => {
        expect(
          screen.getByRole("dialog", { name: /edit group/i }),
        ).toBeInTheDocument();
        expect(screen.getByDisplayValue("Engineering")).toBeInTheDocument();
        expect(screen.getByLabelText(/name/i)).toBeInTheDocument();
        expect(screen.getByLabelText(/description/i)).toBeInTheDocument();
      });

      const cancelButton = screen.getByRole("button", {
        name: /cancel/i,
      });
      await user.click(cancelButton);

      await waitFor(() => {
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      });
    });

    it("tests Delete Group Modal through group dropdown", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { level: 1 }),
        ).toBeInTheDocument();
      });

      const groupsTab = within(container).getByRole("tab", { name: "Groups" });
      await user.click(groupsTab);

      await waitFor(() => {
        expect(within(container).getByText("Engineering")).toBeInTheDocument();
      });

      const groupActionMenus = within(container).getAllByRole("button", {
        name: /open menu/i,
      });
      await user.click(groupActionMenus[0]);

      await waitFor(() => {
        expect(
          screen.getByRole("menuitem", { name: "Delete Group" }),
        ).toBeInTheDocument();
      });

      const deleteGroupButton = screen.getByRole("menuitem", {
        name: "Delete Group",
      });
      await user.click(deleteGroupButton);

      await waitFor(() => {
        expect(
          screen.getByRole("dialog", { name: /delete group/i }),
        ).toBeInTheDocument();
        expect(screen.getAllByText("Engineering").length).toBeGreaterThan(0);
        expect(
          screen.getByRole("button", { name: /delete group/i }),
        ).toBeInTheDocument();
      });

      const cancelButton = screen.getByRole("button", {
        name: /cancel/i,
      });
      await user.click(cancelButton);

      await waitFor(() => {
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      });
    });

    it("tests Group Management Modal through group dropdown", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(
          within(container).getByRole("heading", { level: 1 }),
        ).toBeInTheDocument();
      });

      const groupsTab = within(container).getByRole("tab", { name: "Groups" });
      await user.click(groupsTab);

      await waitFor(() => {
        expect(within(container).getByText("Engineering")).toBeInTheDocument();
      });

      const groupActionMenus = within(container).getAllByRole("button", {
        name: /open menu/i,
      });
      await user.click(groupActionMenus[0]);

      await waitFor(() => {
        expect(
          screen.getByRole("menuitem", { name: "Manage Members" }),
        ).toBeInTheDocument();
      });

      const manageGroupButton = screen.getByRole("menuitem", {
        name: "Manage Members",
      });
      await user.click(manageGroupButton);

      await waitFor(() => {
        expect(screen.getByRole("dialog")).toBeInTheDocument();
        expect(
          screen.getByRole("button", { name: /done/i }),
        ).toBeInTheDocument();
      });

      const doneButton = screen.getByRole("button", {
        name: /done/i,
      });
      await user.click(doneButton);

      await waitFor(() => {
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
      });
    });

    it("retains model-group management in the SSO dashboard", async () => {
      const user = userEvent.setup();
      const { container } = render(<UsersGroups />, { wrapper: createWrapper() });
      await within(container).findByText("Sarah Chen");
      await user.click(within(container).getAllByTitle("Manage groups")[0]);
      const dialog = await screen.findByRole("dialog");
      expect(within(dialog).getByRole("button", { name: /done/i })).toBeInTheDocument();
    });
  });
});
