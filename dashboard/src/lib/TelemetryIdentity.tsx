import { useEffect } from "react";
import { useAuth } from "../contexts/auth";
import { identifyUser } from "./telemetry";

export function TelemetryIdentity() {
  const { user, isAuthenticated, isLoading } = useAuth();

  useEffect(() => {
    if (isLoading) {
      return;
    }
    if (!isAuthenticated || !user) {
      identifyUser(null);
      return;
    }
    identifyUser(user.id, {
      roles: user.roles,
      auth_source: user.auth_source,
    });
  }, [isAuthenticated, isLoading, user]);

  return null;
}
