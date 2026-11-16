import { useCallback, useEffect, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { dwctlApi } from "../../api/control-layer/client";
import { queryKeys } from "../../api/control-layer/keys";
import { AuthContext } from "./auth-context";
import type {
  AuthContextValue,
  AuthState,
  LoginCredentials,
  RegisterCredentials,
} from "./types";
import { useSettings } from "@/contexts";

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps) {
  const { isFeatureEnabled, isMswReady } = useSettings();
  const isDemoMode = isFeatureEnabled("demo");

  const [authState, setAuthState] = useState<AuthState>({
    user: null,
    isAuthenticated: false,
    isLoading: true,
    authMethod: null,
  });

  const queryClient = useQueryClient();

  const checkAuthStatus = useCallback(async () => {
    try {
      setAuthState((prev) => ({ ...prev, isLoading: true }));

      const user = await dwctlApi.users.get("current", { include: "organizations" });
      queryClient.setQueryData(
        queryKeys.users.byId("current", "organizations"),
        user,
      );

      if (user.onboarding_redirect_url) {
        const urlRedirect = new URLSearchParams(window.location.search).get("redirect");
        if (!urlRedirect) {
          try {
            const onboardingKey = `onboarding_completed_${user.id}`;
            if (!localStorage.getItem(onboardingKey)) {
              const onboardingUrl = new URL(user.onboarding_redirect_url, window.location.origin);
              const currentParams = new URLSearchParams(window.location.search);
              currentParams.forEach((v, k) => {
                if (k !== "redirect" && !onboardingUrl.searchParams.has(k)) {
                  onboardingUrl.searchParams.set(k, v);
                }
              });
              localStorage.setItem(onboardingKey, "true");
              window.location.href = onboardingUrl.toString();
              return;
            }
          } catch {
          }
        }
      }

      const authMethod = user.auth_source === "native" ? "native" : "proxy";

      setAuthState({
        user,
        isAuthenticated: true,
        isLoading: false,
        authMethod,
      });
    } catch {
      setAuthState({
        user: null,
        isAuthenticated: false,
        isLoading: false,
        authMethod: null,
      });
    }
  }, [queryClient]);

  useEffect(() => {
    if (isDemoMode && !isMswReady) {
      return;
    }

    checkAuthStatus();
  }, [isDemoMode, isMswReady, checkAuthStatus]);

  const login = async (credentials: LoginCredentials) => {
    await dwctlApi.auth.login(credentials);

    await checkAuthStatus();

    queryClient.invalidateQueries({ queryKey: queryKeys.users.all });
  };

  const register = async (credentials: RegisterCredentials) => {
    await dwctlApi.auth.register(credentials);

    await checkAuthStatus();

    queryClient.invalidateQueries({ queryKey: queryKeys.users.all });
  };

  const logout = async () => {
    if (authState.authMethod === "proxy") {
      window.location.href = "/authentication/logout";
      return;
    }
    try {
      await dwctlApi.auth.logout();
      window.location.href = "/";
    } catch {
      window.location.href = "/authentication/logout";
    }
  };

  const refreshUser = async () => {
    await checkAuthStatus();
  };

  const value: AuthContextValue = {
    ...authState,
    login,
    register,
    logout,
    refreshUser,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
