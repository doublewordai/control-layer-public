import type { Model } from "../api/control-layer/types";

export function isPlaygroundDenied(model: Model): boolean {
  return (
    model.traffic_routing_rules?.some(
      (rule) =>
        rule.action.type === "deny" && rule.api_key_purpose === "playground",
    ) ?? false
  );
}

export function isRealtimeDenied(model: Model): boolean {
  return (
    model.traffic_routing_rules?.some(
      (rule) =>
        rule.action.type === "deny" && rule.api_key_purpose === "realtime",
    ) ?? false
  );
}

export function isBatchDenied(model: Model): boolean {
  return (
    model.traffic_routing_rules?.some(
      (rule) =>
        rule.action.type === "deny" && rule.api_key_purpose === "batch",
    ) ?? false
  );
}
