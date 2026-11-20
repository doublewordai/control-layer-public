#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

scope=(--workspace --all-targets --all-features)
check=false
case "${1:-}" in
    "") ;;
    --check) check=true ;;
    *) echo "usage: $0 [--check]" >&2; exit 2 ;;
esac

if [[ -n "${RUSTC_WRAPPER:-}" ]]; then export SQLX_PREPARE_RUSTC_WRAPPER="$RUSTC_WRAPPER"; fi
export RUSTC_WRAPPER="$PWD/scripts/sqlx-dependencies-offline.sh"

if $check; then
    if cargo sqlx prepare --check --workspace -- "${scope[@]}"; then exit 0; fi
    echo "sqlx prepare: check failed; if .sqlx is out of date, run ./scripts/sqlx-prepare.sh against a migrated database and commit .sqlx" >&2
    exit 1
fi

echo "sqlx prepare: compiling dependencies..." >&2
precompile_log="$(mktemp)"
trap 'rm -f "$precompile_log"' EXIT
SQLX_OFFLINE=true cargo check "${scope[@]}" --keep-going >"$precompile_log" 2>&1 || true
if cargo sqlx prepare --workspace -- "${scope[@]}"; then exit 0; else status=$?; fi
echo "sqlx prepare: output of the offline dependency build:" >&2
tail -n 40 "$precompile_log" >&2
exit "$status"
