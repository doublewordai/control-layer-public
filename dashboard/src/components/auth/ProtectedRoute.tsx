import { Navigate, useLocation } from "react-router-dom";
import type { ReactNode } from "react";
import { useAuth } from "../../contexts/auth";
import { useAuthorization } from "../../utils/authorization";
import { useSettings } from "../../contexts";
import type { FeatureFlags } from "../../contexts/settings/types";

interface ProtectedRouteProps {
  children: ReactNode;
  path: string;
  requiredFeatureFlag?: keyof FeatureFlags;
}

export function ProtectedRoute({
  children,
  path,
  requiredFeatureFlag,
}: ProtectedRouteProps) {
  const { isAuthenticated, isLoading: authLoading } = useAuth();
  const {
    canAccessRoute,
    getFirstAccessibleRoute,
    isLoading: authorizationLoading,
  } = useAuthorization();
  const { isFeatureEnabled } = useSettings();
  const { search } = useLocation();

  if (authLoading || authorizationLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-doubleword-accent-blue"></div>
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to={`/login${search}`} replace />;
  }

  if (requiredFeatureFlag && !isFeatureEnabled(requiredFeatureFlag)) {
    const fallbackRoute = getFirstAccessibleRoute();
    return <Navigate to={`${fallbackRoute}${search}`} replace />;
  }

  if (!canAccessRoute(path)) {
    const fallbackRoute = getFirstAccessibleRoute();
    return <Navigate to={`${fallbackRoute}${search}`} replace />;
  }

  return <>{children}</>;
}
