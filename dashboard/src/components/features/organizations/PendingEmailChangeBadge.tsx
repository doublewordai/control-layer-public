import { MailWarning } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { PendingEmailChange } from "@/api/control-layer/types";
import { describeWaitingOn } from "./pendingEmailChange";

interface PendingEmailChangeBadgeProps {
  pending?: PendingEmailChange | null;
  currentEmail?: string | null;
}

export function PendingEmailChangeBadge({
  pending,
  currentEmail,
}: PendingEmailChangeBadgeProps) {
  if (!pending) return null;

  const expires = new Date(pending.expires_at).toLocaleString();
  const waiting = describeWaitingOn(pending, currentEmail ?? "current address");
  return (
    <Badge
      variant="outline"
      className="border-yellow-300 bg-yellow-50 text-yellow-800 whitespace-normal"
      title={`Both the current and the new address must confirm before the email changes. Links expire ${expires}.`}
    >
      <MailWarning />
      Pending change to {pending.new_email} · {waiting}
    </Badge>
  );
}
