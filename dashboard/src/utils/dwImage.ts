
export type DwImgSegment =
  | { kind: "text"; value: string }
  | { kind: "token"; raw: string; sha256: string; ref: string };

export function dwImageUrl(ref: string): string {
  return `/admin/api/v1/images/${ref}`;
}

export function splitDwImgTokens(text: string): DwImgSegment[] {
  const re = /dw-img:\/\/([a-f0-9]{64})(\.[a-f0-9]{32})?(?![a-f0-9]|\.[a-f0-9])/gi;
  const segments: DwImgSegment[] = [];
  let cursor = 0;
  for (const match of text.matchAll(re)) {
    const start = match.index ?? 0;
    if (start > cursor) {
      segments.push({ kind: "text", value: text.slice(cursor, start) });
    }
    const sha256 = match[1].toLowerCase();
    const ref = sha256 + (match[2] ?? "").toLowerCase();
    segments.push({ kind: "token", raw: match[0], sha256, ref });
    cursor = start + match[0].length;
  }
  if (cursor < text.length) {
    segments.push({ kind: "text", value: text.slice(cursor) });
  }
  return segments;
}
