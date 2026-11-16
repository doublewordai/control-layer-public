import { lazy, type ComponentType, type LazyExoticComponent } from "react";

type AnyComponent = ComponentType<any>;

const RELOAD_KEY = "dashboard-chunk-reload-attempted";

let reloadAttempted = false;

function storageGet(key: string): string | null {
  try {
    return sessionStorage.getItem(key);
  } catch {
    return null;
  }
}
function storageSet(key: string, value: string): void {
  try {
    sessionStorage.setItem(key, value);
  } catch {
  }
}
function storageRemove(key: string): void {
  try {
    sessionStorage.removeItem(key);
  } catch {
  }
}

export function lazyWithRetry<T extends AnyComponent>(
  factory: () => Promise<{ default: T }>,
): LazyExoticComponent<T> {
  return lazy(async () => {
    try {
      const mod = await factory();
      reloadAttempted = false;
      storageRemove(RELOAD_KEY);
      return mod;
    } catch (initialError) {
      try {
        const mod = await factory();
        reloadAttempted = false;
        storageRemove(RELOAD_KEY);
        return mod;
      } catch (retryError) {
        if (
          typeof window !== "undefined" &&
          !reloadAttempted &&
          !storageGet(RELOAD_KEY)
        ) {
          reloadAttempted = true;
          storageSet(RELOAD_KEY, "1");
          window.location.reload();
          return new Promise<{ default: T }>(() => {});
        }
        console.error("Failed to load route chunk:", initialError, retryError);
        throw retryError;
      }
    }
  });
}
