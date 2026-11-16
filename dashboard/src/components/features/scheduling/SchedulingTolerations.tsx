import { AlertTriangle, ShieldCheck } from "lucide-react";
import type { SchedulingTolerationPreset } from "@/api/control-layer/types";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { InfoTip } from "@/components/ui/info-tip";
import { DEFAULT_TOLERATION_PRESET } from "@/utils/schedulingPresets";

interface SchedulingTolerationsProps {
  preset: string | null | undefined;
  onChange: (next: string | null) => void;
  presets: SchedulingTolerationPreset[] | undefined;
  canEdit: boolean;
}

export function SchedulingTolerations({
  preset,
  onChange,
  presets,
  canEdit,
}: SchedulingTolerationsProps) {
  const available = presets ?? [];
  const value = preset ?? DEFAULT_TOLERATION_PRESET;
  const selected = available.find((p) => p.id === preset);

  const hasPresets = available.length > 0;

  const staleSelection = preset != null && selected === undefined;

  return (
    <div
      className="space-y-3 border-t pt-4"
      data-testid="scheduling-tolerations"
    >
      <div className="flex items-start gap-2">
        <ShieldCheck className="mt-0.5 h-4 w-4 text-gray-500" />
        <div className="grid gap-1">
          <div className="flex items-center gap-1">
            <Label
              htmlFor="toleration-preset"
              className="text-sm font-medium text-gray-700"
            >
              Scheduling tolerations
            </Label>
            <InfoTip>
              <p className="text-sm">
                Pins every inference request from this account to a scheduling
                toleration preset. A backend that honours tolerations uses it
                to decide which capacity may serve the request. Separate from
                the gateway's provider fallback setting above.
              </p>
            </InfoTip>
          </div>
          <p className="text-xs text-gray-500">
            Where the inference backend may place this account's requests.
          </p>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <Select
          value={value}
          onValueChange={(next) =>
            onChange(next === DEFAULT_TOLERATION_PRESET ? null : next)
          }
          disabled={!canEdit}
        >
          <SelectTrigger
            id="toleration-preset"
            className="w-full"
            aria-label="Scheduling tolerations preset"
          >
            <SelectValue placeholder="Default (no tolerations pinned)" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={DEFAULT_TOLERATION_PRESET}>
              Default (no tolerations pinned)
            </SelectItem>
            {available.map((p) => (
              <SelectItem key={p.id} value={p.id}>
                {p.label}
              </SelectItem>
            ))}
            {staleSelection && (
              <SelectItem value={preset as string}>
                {preset} (not configured)
              </SelectItem>
            )}
          </SelectContent>
        </Select>
        {selected && (
          <InfoTip>
            <p className="text-sm">{selected.description}</p>
          </InfoTip>
        )}
      </div>

      {selected && (
        <p className="text-xs text-gray-500">{selected.description}</p>
      )}

      {hasPresets && (
        <p className="flex items-start gap-1.5 text-xs text-amber-600">
          <AlertTriangle
            className="mt-0.5 h-3.5 w-3.5 shrink-0"
            aria-hidden="true"
          />
          <span data-testid="toleration-pinning-caution">
            Pinning an account restricts where its requests can be placed, and
            can make scheduling harder for accounts sharing the same capacity,
            depending on how the backend treats tolerations. Pin as few accounts
            as possible and only when a customer requires it.
          </span>
        </p>
      )}

      {staleSelection && (
        <p className="text-xs text-amber-600" role="alert">
          This account is pinned to "{preset}", which is no longer a configured
          preset. Until it is re-pointed, the most restrictive tolerations are
          applied.
        </p>
      )}
    </div>
  );
}
