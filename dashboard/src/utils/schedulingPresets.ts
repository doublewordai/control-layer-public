import type { SchedulingTolerationPreset } from "@/api/control-layer/types";

export const DEFAULT_TOLERATION_PRESET = "__default__";

export function presetBadgeLabel(
  preset: string | null | undefined,
  presets: SchedulingTolerationPreset[] | undefined,
): string | null {
  if (preset == null) return null;
  const match = presets?.find((p) => p.id === preset);
  return match?.label ?? preset;
}
