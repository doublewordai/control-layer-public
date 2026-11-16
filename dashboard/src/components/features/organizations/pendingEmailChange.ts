import type { PendingEmailChange } from "@/api/control-layer/types";

export function waitingOn(
  pending: PendingEmailChange,
  currentEmail: string,
): string[] {
  const outstanding: string[] = [];
  if (!pending.old_email_confirmed_at) outstanding.push(currentEmail);
  if (!pending.new_email_confirmed_at) outstanding.push(pending.new_email);
  return outstanding;
}

export function describeWaitingOn(
  pending: PendingEmailChange,
  currentEmail: string,
): string {
  const outstanding = waitingOn(pending, currentEmail);
  if (outstanding.length === 0) {
    return "both addresses have confirmed; applying";
  }
  if (outstanding.length === 1) {
    return `waiting on ${outstanding[0]} to confirm`;
  }
  return `waiting on both ${outstanding[0]} and ${outstanding[1]} to confirm`;
}
