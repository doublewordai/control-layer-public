import { createContext } from "react";
import type { OrganizationSummary } from "../../api/control-layer/types";

export interface OrganizationContextValue {
  activeOrganizationId: string | null;
  activeOrganization: OrganizationSummary | null;
  isOrgContext: boolean;
  setActiveOrganization: (orgId: string | null) => Promise<void>;
}

export const OrganizationContext =
  createContext<OrganizationContextValue | null>(null);
