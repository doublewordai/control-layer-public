import type { Role } from "../api/control-layer/types";

export const AVAILABLE_ROLES: Role[] = [
  "PlatformManager",
  "RequestViewer",
  "StandardUser",
  "BillingManager",
  "BatchAPIUser",
  "ConnectionsUser",
];

export const SUBSET_ROLES: Role[] = [
  "BatchAPIUser",
  "BillingManager",
  "ConnectionsUser",
];

export const EDITABLE_ROLES: Role[] = [
  "PlatformManager",
  "RequestViewer",
  "BillingManager",
  "BatchAPIUser",
  "ConnectionsUser",
];

export const formatRoleForDisplay = (role: Role): string => {
  const displayNames: Record<Role, string> = {
    PlatformManager: "Platform Manager",
    RequestViewer: "Request Viewer",
    StandardUser: "Standard User",
    BillingManager: "Billing Manager",
    BatchAPIUser: "Batch API User",
    ConnectionsUser: "Connections User",
  };
  return displayNames[role] || role;
};

export const isAdmin = (isAdminFlag: boolean): boolean => {
  return isAdminFlag;
};

export const getRoleDisplayName = (role: Role): string => {
  return formatRoleForDisplay(role);
};
