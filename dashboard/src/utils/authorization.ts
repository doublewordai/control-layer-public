import { useMemo } from "react";
import type { ConfigResponse, Role } from "../api/control-layer/types";
import { useConfig, useUser } from "../api/control-layer/hooks";

export type PagePermission =
  | "models"
  | "endpoints"
  | "playground"
  | "analytics"
  | "cost-management"
  | "users-groups"
  | "organizations"
  | "my-organization"
  | "api-keys"
  | "settings"
  | "profile"
  | "manage-groups"
  | "manage-models"
  | "batches"
  | "usage"
  | "connections";

const ROLE_PERMISSIONS: Record<Role, PagePermission[]> = {
  PlatformManager: [
    "models",
    "endpoints",
    "playground",
    "analytics",
    "cost-management",
    "users-groups",
    "organizations",
    "my-organization",
    "api-keys",
    "settings",
    "profile",
    "manage-groups",
    "manage-models",
    "batches",
    "usage",
    "connections",
  ],
  StandardUser: [
    "models",
    "api-keys",
    "playground",
    "cost-management",
    "profile",
    "organizations",
    "my-organization",
    "usage",
  ],
  RequestViewer: [
    "models",
    "endpoints",
    "analytics",
    "cost-management",
    "profile",
    "settings",
    "usage",
  ],
  BillingManager: ["models", "cost-management", "profile", "usage"],
  BatchAPIUser: [
    "models",
    "api-keys",
    "playground",
    "cost-management",
    "profile",
    "batches",
    "my-organization",
    "usage",
  ],
  ConnectionsUser: [
    "connections",
    "batches",
  ],
};

export const ROUTE_PERMISSIONS: Record<string, PagePermission> = {
  "/models": "models",
  "/models/manage": "manage-models",
  "/endpoints": "endpoints",
  "/playground": "playground",
  "/analytics": "analytics",
  "/cost-management": "cost-management",
  "/users-groups": "users-groups",
  "/organization": "my-organization",
  "/api-keys": "api-keys",
  "/settings": "settings",
  "/system": "settings",
  "/profile": "profile",
  "/batches": "batches",
  "/responses": "batches",
  "/usage": "usage",
  "/connections": "connections",
};

export function hasPermission(
  userRoles: Role[],
  permission: PagePermission,
): boolean {
  return userRoles.some((role) => ROLE_PERMISSIONS[role]?.includes(permission));
}

function isRouteEnabledByConfig(
  path: string,
  config?: ConfigResponse,
): boolean {
  if (!config) {
    return true;
  }

  if (path === "/responses") {
    return Boolean(
      config.batches?.enabled && config.batches.async_requests?.enabled,
    );
  }

  if (path === "/batches") {
    return Boolean(config.batches?.enabled);
  }

  return true;
}

export function canAccessRoute(
  userRoles: Role[],
  path: string,
  config?: ConfigResponse,
): boolean {
  const permission = ROUTE_PERMISSIONS[path];
  if (!isRouteEnabledByConfig(path, config)) {
    return false;
  }
  if (!permission) {
    return true;
  }
  return hasPermission(userRoles, permission);
}

export function getFirstAccessibleRoute(
  userRoles: Role[],
  config?: ConfigResponse,
): string {
  const fallbackOrder: string[] = [
    "/models",
    "/responses",
    "/batches",
    "/playground",
    "/api-keys",
    "/profile",
  ];

  for (const route of fallbackOrder) {
    if (canAccessRoute(userRoles, route, config)) {
      return route;
    }
  }

  return "/profile";
}

export function useAuthorization() {
  const { data: currentUser, isLoading: userLoading } = useUser("current");
  const { data: config, isLoading: configLoading } = useConfig();
  const isLoading = userLoading || configLoading;

  const permissions = useMemo(() => {
    if (!currentUser?.roles) {
      return {
        isLoading,
        userRoles: [] as Role[],
        isAdmin: false,
        hasPermission: () => false,
        canAccessRoute: () => false,
        getFirstAccessibleRoute: (): string => "/profile",
      };
    }

    const userRoles = currentUser.roles;
    const permissionRoles: Role[] = currentUser.is_admin
      ? [...userRoles, "PlatformManager"]
      : userRoles;

    return {
      isLoading,
      userRoles: permissionRoles,
      hasPermission: (permission: PagePermission) =>
        hasPermission(permissionRoles, permission),
      canAccessRoute: (path: string) => canAccessRoute(permissionRoles, path, config),
      getFirstAccessibleRoute: (): string =>
        getFirstAccessibleRoute(permissionRoles, config),
    };
  }, [config, currentUser, isLoading]);

  return permissions;
}
