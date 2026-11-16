#!/usr/bin/env bash

set -euo pipefail

DIRS=("$@")
if [ ${#DIRS[@]} -eq 0 ]; then
    DIRS=("dwctl/src")
fi

if command -v rg >/dev/null 2>&1; then
    search() { rg --no-heading --line-number --color never -e "$1" "${DIRS[@]}"; }
else
    search() { grep -rEn -- "$1" "${DIRS[@]}"; }
fi

WORD='(body|response|response_body|request_body|messages|prompt|completion|chunk|delta|payload|content)'

PAT_SAMPLE='\b(body_sample|data_sample|response_preview|prompt_sample|completion_sample|raw_body|response_body_sample)\b'
PAT_LOSSY='=[[:space:]]*[%?][[:space:]]*String::from_utf8_lossy'
PAT_FIELD="\b${WORD}\b[[:space:]]*=[[:space:]]*[%?]"
PAT_VALUE="=[[:space:]]*[%?]${WORD}\b"

violations=""
for pat in "$PAT_SAMPLE" "$PAT_LOSSY" "$PAT_FIELD" "$PAT_VALUE"; do
    hits="$(search "$pat" 2>/dev/null | grep -v 'zdr-allow' || true)"
    if [ -n "$hits" ]; then
        violations+="$hits"$'\n'
    fi
done

if [ -n "${violations//[$'\n']/}" ]; then
    echo "❌ ZDR no-payload-logging guard: potential prompt/response payload in a log statement." >&2
    echo "   Replace the payload with length/status metadata, or annotate a verified" >&2
    echo "   false positive with '// zdr-allow: <reason>'." >&2
    echo >&2
    printf '%s\n' "$violations" | sed '/^$/d' | sort -u >&2
    exit 1
fi

echo "✅ ZDR no-payload-logging guard: clean (${DIRS[*]})"
