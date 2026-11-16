import type { Batch } from "./types";

export function batchModelName(batch: Batch): string | null {
  const fromField = batch.model?.trim();
  if (fromField) return fromField;
  const fromMeta = batch.metadata?.model?.trim();
  if (fromMeta) return fromMeta;
  return null;
}
