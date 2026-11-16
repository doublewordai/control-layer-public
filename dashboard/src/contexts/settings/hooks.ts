import { useContext } from "react";
import { SettingsContext, type SettingsContextType } from "./context";

export function useSettings(): SettingsContextType {
  const context = useContext(SettingsContext);
  if (context === undefined) {
    throw new Error("useSettings must be used within a SettingsProvider");
  }
  return context;
}
