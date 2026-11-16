import type { SpendLimitInterval } from "../../../../api/control-layer/types";
import { formatDollars } from "../../../../utils/money";


export function formatCredits(value: string | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  const n = Number(value);
  if (!Number.isFinite(n)) return "—";
  const maxDecimalPlaces = n !== 0 && Math.abs(n) < 0.01 ? 4 : 2;
  return formatDollars(n, maxDecimalPlaces);
}

export function nextResetBoundary(
  interval: SpendLimitInterval | null | undefined,
  now: Date = new Date(),
): Date | null {
  if (!interval) return null;
  const y = now.getUTCFullYear();
  const m = now.getUTCMonth();
  const d = now.getUTCDate();
  switch (interval) {
    case "daily":
      return new Date(Date.UTC(y, m, d + 1));
    case "weekly": {
      const dow = now.getUTCDay();
      const daysUntilMonday = dow === 0 ? 1 : 8 - dow;
      return new Date(Date.UTC(y, m, d + daysUntilMonday));
    }
    case "monthly":
      return new Date(Date.UTC(y, m + 1, 1));
  }
}

export function formatResetInstant(date: Date | string): string {
  const d = typeof date === "string" ? new Date(date) : date;
  const day = d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  });
  const time = d.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZone: "UTC",
  });
  return `${day}, ${time} UTC`;
}

export function resetPreviewLine(
  interval: SpendLimitInterval | null | undefined,
  now: Date = new Date(),
): string {
  const boundary = nextResetBoundary(interval, now);
  if (!boundary) {
    return "No automatic reset — the cap counts all spend from when it is set.";
  }
  return `Next resets ${formatResetInstant(boundary)}.`;
}

export function limitPeriodLabel(
  interval: SpendLimitInterval | null | undefined,
): string {
  switch (interval) {
    case "daily":
      return "Daily limit";
    case "weekly":
      return "Weekly limit";
    case "monthly":
      return "Monthly limit";
    default:
      return "One-off limit";
  }
}
