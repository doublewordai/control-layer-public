import React, { useState, useEffect } from "react";
import { SettingsContext, type SettingsContextType } from "./context";
import type { FeatureFlags, AppSettings } from "./types";

const STORAGE_KEY = "app-settings";

const DEFAULT_SETTINGS: AppSettings = {
  apiBaseUrl: "/admin/api/v1",
  features: {
    demo: false,
  },
};

function parseUrlFlags(): Partial<AppSettings> {
  const urlParams = new URLSearchParams(window.location.search);
  const settings: Partial<AppSettings> = {};

  const flagsParam = urlParams.get("flags");
  if (flagsParam !== null) {
    const flagList = flagsParam.split(",").map((f) => f.trim());

    settings.features = {
      demo: flagList.includes("demo"),
    };
  }

  return settings;
}

function loadSettings(): AppSettings {
  const urlSettings = parseUrlFlags();

  const storedSettings = localStorage.getItem(STORAGE_KEY);
  let localSettings: Partial<AppSettings> = {};

  if (storedSettings) {
    try {
      localSettings = JSON.parse(storedSettings);
    } catch {
      console.warn("Failed to parse stored settings, using defaults");
    }
  }

  return {
    apiBaseUrl:
      import.meta.env.VITE_API_BASE_URL ||
      localSettings.apiBaseUrl ||
      DEFAULT_SETTINGS.apiBaseUrl,
    features: {
      demo:
        urlSettings.features?.demo ??
        localSettings.features?.demo ??
        DEFAULT_SETTINGS.features.demo,
    },
    demoConfig: localSettings.demoConfig,
  };
}

function saveSettings(settings: AppSettings): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
}

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<AppSettings>(loadSettings);
  const [isMswReady, setIsMswReady] = useState(false);

  useEffect(() => {
    saveSettings(settings);
  }, [settings]);

  useEffect(() => {
    setIsMswReady(!settings.features.demo);
  }, [settings.features.demo]);

  const toggleFeature = async (
    feature: keyof FeatureFlags,
    enabled: boolean,
  ) => {
    setSettings((prev) => ({
      ...prev,
      features: {
        ...prev.features,
        [feature]: enabled,
      },
    }));

    if (feature === "demo") {
      if (!enabled && "serviceWorker" in navigator) {
        const registrations = await navigator.serviceWorker.getRegistrations();
        for (const registration of registrations) {
          await registration.unregister();
        }
        window.location.reload();
      } else if (
        enabled &&
        !("serviceWorker" in navigator && navigator.serviceWorker.controller)
      ) {
        window.location.reload();
      }
    }
  };

  const isFeatureEnabled = (feature: keyof FeatureFlags): boolean => {
    return settings.features[feature];
  };

  const updateDemoConfig = (config: Partial<import("./types").DemoConfig>) => {
    setSettings((prev) => ({
      ...prev,
      demoConfig: {
        ...prev.demoConfig,
        ...config,
      },
    }));
  };

  const setMswReady = (ready: boolean) => {
    setIsMswReady(ready);
  };

  const contextValue: SettingsContextType = {
    settings,
    toggleFeature,
    isFeatureEnabled,
    updateDemoConfig,
    isMswReady,
    setMswReady,
  };

  return (
    <SettingsContext.Provider value={contextValue}>
      {children}
    </SettingsContext.Provider>
  );
}
