export function formatMemory(memoryGb: number): string {
  if (!isFinite(memoryGb) || isNaN(memoryGb)) return "N/A";
  if (memoryGb < 0) return "0GB";

  if (memoryGb >= 1000) {
    return `${(memoryGb / 1000).toFixed(1)}TB`;
  }
  return `${memoryGb}GB`;
}

export function formatNumber(num: number): string {
  if (!isFinite(num) || isNaN(num)) return "N/A";
  if (num < 0) return "0";

  if (num >= 1_000_000_000) {
    return `${(num / 1_000_000_000).toFixed(1)}B`;
  }
  if (num >= 1_000_000) {
    return `${(num / 1_000_000).toFixed(1)}M`;
  }
  if (num >= 1_000) {
    return `${(num / 1_000).toFixed(1)}K`;
  }
  return num.toString();
}

export function formatTokens(tokens: number): string {
  if (!isFinite(tokens) || isNaN(tokens)) return "N/A";
  if (tokens < 0) return "0";

  if (tokens >= 1_000_000) return `${(tokens / 1_000_000).toFixed(1)}M`;
  if (tokens >= 1_000) return `${(tokens / 1_000).toFixed(1)}K`;
  return tokens.toString();
}

export function formatDuration(ms: number): string {
  if (!isFinite(ms) || isNaN(ms)) return "N/A";
  if (ms < 0) return "0ms";

  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

export function formatLongDuration(ms: number): string {
  if (!isFinite(ms) || isNaN(ms)) return "N/A";
  if (ms < 0) return "0s";
  if (ms < 1000) return "< 1s";

  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) {
    const remainingHours = hours % 24;
    if (remainingHours > 0) {
      return `${days}d ${remainingHours}h`;
    }
    return `${days}d`;
  }

  if (hours > 0) {
    const remainingMinutes = minutes % 60;
    if (remainingMinutes > 0) {
      return `${hours}h ${remainingMinutes}m`;
    }
    return `${hours}h`;
  }

  if (minutes > 0) {
    const remainingSeconds = seconds % 60;
    if (remainingSeconds > 0) {
      return `${minutes}m ${remainingSeconds}s`;
    }
    return `${minutes}m`;
  }

  return `${seconds}s`;
}

export function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  if (isNaN(date.getTime())) return "Invalid date";
  return date.toLocaleString();
}

export function formatBytes(bytes: number): string {
  if (!isFinite(bytes) || isNaN(bytes)) return "N/A";
  if (bytes < 0) return "0 Bytes";
  if (bytes === 0) return "0 Bytes";

  const k = 1000;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  const sizeIndex = Math.min(i, sizes.length - 1);

  return (
    Math.round((bytes / Math.pow(k, sizeIndex)) * 100) / 100 +
    " " +
    sizes[sizeIndex]
  );
}

export function formatLatency(ms?: number): string {
  if (!ms) return "N/A";
  if (ms >= 1000) {
    return `${(ms / 1000).toFixed(1)}s`;
  }
  return `${Math.round(ms)}ms`;
}

export function formatRelativeTime(dateString?: string): string {
  if (!dateString) return "Never";

  const date = new Date(dateString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();

  const diffMinutes = Math.floor(diffMs / (1000 * 60));
  const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

  if (diffMinutes < 1) return "Just now";
  if (diffMinutes < 60) return `${diffMinutes}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;

  return date.toLocaleDateString();
}

export function formatContextLength(tokens: number): string {
  if (tokens >= 1024) {
    return `${Math.round(tokens / 1024)}k`;
  }
  return `${tokens}`;
}

export function getTariffDisplayName(
  apiKeyPurpose: string | null | undefined,
  completionWindow: string | null | undefined,
): string {
  if (apiKeyPurpose === "realtime") return "Realtime";
  if (apiKeyPurpose === "playground") return "Playground";
  if (apiKeyPurpose === "batch") {
    if (completionWindow === "1h") return "Async";
    return "Batch";
  }
  return "Realtime";
}

const TARIFF_SORT_ORDER: Record<string, number> = {
  realtime: 0,
  "batch:1h": 1,
  "batch:24h": 2,
  batch: 3,
};

function tariffSortKey(
  apiKeyPurpose: string | null | undefined,
  completionWindow: string | null | undefined,
): number {
  if (apiKeyPurpose === "batch" && completionWindow) {
    return TARIFF_SORT_ORDER[`batch:${completionWindow}`] ?? 3;
  }
  return TARIFF_SORT_ORDER[apiKeyPurpose ?? "realtime"] ?? 0;
}

export function getUserFacingTariffs<
  T extends {
    is_active?: boolean;
    api_key_purpose?: string | null;
    completion_window?: string | null;
  },
>(tariffs: T[]): T[] {
  return tariffs
    .filter((t) => t.api_key_purpose !== "playground" && t.is_active !== false)
    .sort(
      (a, b) =>
        tariffSortKey(a.api_key_purpose, a.completion_window) -
        tariffSortKey(b.api_key_purpose, b.completion_window),
    );
}

function deniedPurposes<
  M extends {
    traffic_routing_rules?:
      | Array<{
          api_key_purpose?: string | null;
          action: { type: string; [key: string]: unknown };
        }>
      | null;
  },
>(model: M): Set<string> {
  const denied = new Set<string>();
  for (const rule of model.traffic_routing_rules ?? []) {
    if (rule.action.type === "deny" && rule.api_key_purpose != null) {
      denied.add(rule.api_key_purpose);
    }
  }
  return denied;
}

export function getVisibleTariffsForModel<
  T extends {
    is_active?: boolean;
    api_key_purpose?: string | null;
    completion_window?: string | null;
  },
>(model: {
  tariffs?: T[] | null;
  traffic_routing_rules?:
    | Array<{
        api_key_purpose?: string | null;
        action: { type: string; [key: string]: unknown };
      }>
    | null;
}): T[] {
  const denied = deniedPurposes(model);
  return getUserFacingTariffs(model.tariffs ?? []).filter(
    (t) => t.api_key_purpose == null || !denied.has(t.api_key_purpose),
  );
}

export function formatTariffPrice(
  pricePerToken: string | number | null | undefined,
): string {
  if (!pricePerToken || pricePerToken === "0" || pricePerToken === 0) {
    return "$0";
  }

  const pricePerMillion = parseFloat(String(pricePerToken)) * 1000000;

  return `$${pricePerMillion.toFixed(2)}`;
}

export function formatTariffPricing(
  inputPrice: string | number | null | undefined,
  outputPrice: string | number | null | undefined,
): string {
  return `${formatTariffPrice(inputPrice)} / ${formatTariffPrice(outputPrice)}`;
}
