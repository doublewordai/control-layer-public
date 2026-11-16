import { render, screen, act, cleanup, within } from "@testing-library/react";
import { SettingsProvider } from "./SettingsContext";
import { useSettings } from "./hooks";

function TestComponent() {
  const { settings, toggleFeature, isFeatureEnabled } = useSettings();

  return (
    <div>
      <div data-testid="api-base-url">{settings.apiBaseUrl}</div>
      <div data-testid="demo-enabled">{settings.features.demo.toString()}</div>
      <div data-testid="demo-feature-check">
        {isFeatureEnabled("demo").toString()}
      </div>
      <button
        data-testid="toggle-demo"
        onClick={() => toggleFeature("demo", !settings.features.demo)}
      >
        Toggle Demo
      </button>
    </div>
  );
}

describe("SettingsContext", () => {
  beforeEach(() => {
    window.location.search = "";
  });

  afterEach(() => {
    cleanup();
    localStorage.clear();
    vi.unstubAllGlobals();
  });

  it("provides default settings when no localStorage or URL params", () => {
    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("api-base-url")).toHaveTextContent(
      "/admin/api/v1",
    );
    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "false",
    );
    expect(
      within(container).getByTestId("demo-feature-check"),
    ).toHaveTextContent("false");
  });

  it("loads settings from localStorage", () => {
    const storedSettings = {
      apiBaseUrl: "/custom/api",
      features: {
        demo: true,
      },
    };

    localStorage.setItem("app-settings", JSON.stringify(storedSettings));

    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("api-base-url")).toHaveTextContent(
      "/custom/api",
    );
    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "true",
    );
  });

  it("URL params completely override localStorage settings", () => {
    const storedSettings = {
      apiBaseUrl: "/stored/api",
      features: {
        demo: true,
      },
    };
    localStorage.setItem("app-settings", JSON.stringify(storedSettings));

    vi.stubGlobal(
      "URLSearchParams",
      vi.fn(function () {
        return {
          get: vi.fn((key: string) => (key === "flags" ? "demo" : null)),
        };
      }),
    );

    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "true",
    );
    expect(within(container).getByTestId("api-base-url")).toHaveTextContent(
      "/stored/api",
    );
  });

  it("toggles feature flags and saves to localStorage", async () => {
    const mockReload = vi.fn();
    Object.defineProperty(window, "location", {
      value: { ...window.location, reload: mockReload },
      writable: true,
    });

    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "false",
    );

    await act(async () => {
      screen.getByTestId("toggle-demo").click();
    });

    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "true",
    );
    expect(
      within(container).getByTestId("demo-feature-check"),
    ).toHaveTextContent("true");
  });

  it("demo mode toggle triggers automatic reload", async () => {
    const originalLocation = window.location;
    const originalNavigator = window.navigator;

    Object.defineProperty(window, "location", {
      value: { reload: vi.fn() },
      writable: true,
    });

    Object.defineProperty(window, "navigator", {
      value: {
        ...originalNavigator,
        serviceWorker: {
          getRegistrations: vi
            .fn()
            .mockResolvedValue([
              { unregister: vi.fn().mockResolvedValue(true) },
            ]),
          controller: null,
        },
      },
      writable: true,
    });

    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "false",
    );
    expect(window.location.reload).not.toHaveBeenCalled();

    await act(async () => {
      screen.getByTestId("toggle-demo").click();
    });

    expect(window.location.reload).toHaveBeenCalledTimes(1);

    vi.mocked(window.location.reload).mockClear();

    await act(async () => {
      screen.getByTestId("toggle-demo").click();
    });

    expect(window.location.reload).toHaveBeenCalledTimes(1);

    Object.defineProperty(window, "location", {
      value: originalLocation,
      writable: true,
    });
    Object.defineProperty(window, "navigator", {
      value: originalNavigator,
      writable: true,
    });
  });

  it("handles malformed localStorage data gracefully", () => {
    localStorage.setItem("app-settings", "invalid json");

    const { container } = render(
      <SettingsProvider>
        <TestComponent />
      </SettingsProvider>,
    );

    expect(within(container).getByTestId("demo-enabled")).toHaveTextContent(
      "false",
    );
  });

  it("throws error when useSettings is used outside provider", () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    expect(() => {
      render(<TestComponent />);
    }).toThrow("useSettings must be used within a SettingsProvider");

    consoleSpy.mockRestore();
  });
});
