#!/usr/bin/env bash
[[ -n "${CARGO_PRIMARY_PACKAGE:-}" ]] || export SQLX_OFFLINE=true
if [[ -n "${SQLX_PREPARE_RUSTC_WRAPPER:-}" ]]; then
    exec "$SQLX_PREPARE_RUSTC_WRAPPER" "$@"
fi
exec "$@"
