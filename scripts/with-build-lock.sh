#!/usr/bin/env bash
set -euo pipefail

if (($# == 0)); then
    echo "usage: $0 <command> [args...]" >&2
    exit 2
fi

if [[ -n "${DW_BUILD_LOCK_HELD:-}" || "${DW_NO_BUILD_LOCK:-}" == 1 || -n "${CI:-}" ]]; then
    export DW_BUILD_LOCK_HELD=1
    exec "$@"
fi
export DW_BUILD_LOCK_HELD=1

if ! command -v flock >/dev/null 2>&1; then
    echo "build lock: flock not found (macOS: brew install flock); running unlocked" >&2
    exec "$@"
fi

lock="${DW_BUILD_LOCK_FILE:-/tmp/control-layer-build.lock}"
timeout="${DW_BUILD_LOCK_TIMEOUT:-3600}"
conflict=75

if ! flock -n "$lock" true; then
    echo "build lock: waiting for $lock (another heavy cargo job is running; DW_NO_BUILD_LOCK=1 skips)" >&2
fi

status=0
flock -o -w "$timeout" -E "$conflict" "$lock" "$@" || status=$?
if ((status == conflict)); then
    echo "build lock: gave up after ${timeout}s waiting for $lock (or the command exited $conflict)" >&2
fi
exit "$status"
