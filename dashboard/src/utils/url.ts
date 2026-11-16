const PRESERVED_PARAM_PREFIXES = ["utm_"];
const PRESERVED_PARAM_NAMES = new Set(["gclid", "fbclid", "ref", "source"]);

function isPreservedParam(key: string): boolean {
  if (PRESERVED_PARAM_NAMES.has(key)) return true;
  return PRESERVED_PARAM_PREFIXES.some((prefix) => key.startsWith(prefix));
}

export function mergePreservedParams(
  target: string,
  searchParams: URLSearchParams,
): string {
  const preserved = new URLSearchParams();
  searchParams.forEach((value, key) => {
    if (key !== "redirect" && isPreservedParam(key)) {
      preserved.set(key, value);
    }
  });

  const qs = preserved.toString();
  if (!qs) return target;

  const separator = target.includes("?") ? "&" : "?";
  return `${target}${separator}${qs}`;
}
