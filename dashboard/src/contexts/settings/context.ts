import { createContext } from "react";
import type { FeatureFlags, AppSettings, DemoConfig } from "./types";

export interface SettingsContextType {
  settings: AppSettings;
  toggleFeature: (feature: keyof FeatureFlags, enabled: boolean) => void;
  isFeatureEnabled: (feature: keyof FeatureFlags) => boolean;
  updateDemoConfig: (config: Partial<DemoConfig>) => void;
  isMswReady: boolean;
  setMswReady: (ready: boolean) => void;
}

export const SettingsContext = createContext<SettingsContextType | undefined>(
  undefined,
);
