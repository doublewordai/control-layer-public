#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
original_args=("$@")

watch=false
coverage=false
no_run=false
doctests=true
args=()
doc_args=()
default_workspace=true
default_all_features=true
default_deps_dir=true
while (($#)); do
    case "$1" in
        --release|--cargo-profile|--cargo-profile=*|--target|--target=*) default_deps_dir=false ;;
    esac
    case "$1" in
        -p|--package|-p?*|--package=*|--workspace) default_workspace=false ;;
        -F|--features|-F?*|--features=*|--no-default-features|--all-features) default_all_features=false ;;
    esac
    case "$1" in
        --watch) watch=true; shift ;;
        --coverage) coverage=true; shift ;;
        --no-doctests) doctests=false; shift ;;
        --no-run) no_run=true; args+=("$1"); shift ;;
        -p|--package|--exclude|-F|--features|--target)
            if (($# < 2)); then echo "$1 requires a value" >&2; exit 2; fi
            args+=("$1" "$2"); doc_args+=("$1" "$2"); shift 2 ;;
        --cargo-profile)
            if (($# < 2)); then echo "$1 requires a value" >&2; exit 2; fi
            args+=("$1" "$2"); doc_args+=(--profile "$2"); shift 2 ;;
        --release|--locked|--offline|--no-default-features|--all-features|--workspace|-p?*|-F?*|--package=*|--exclude=*|--features=*|--target=*)
            args+=("$1"); doc_args+=("$1"); shift ;;
        --cargo-profile=*)
            args+=("$1"); doc_args+=("--profile=${1#*=}"); shift ;;
        --) args+=("$@"); break ;;
        *) args+=("$1"); shift ;;
    esac
done

if ! $watch && [[ -z "${DW_BUILD_LOCK_HELD:-}" ]]; then
    exec ./scripts/with-build-lock.sh ./scripts/test-rust.sh ${original_args[@]+"${original_args[@]}"}
fi

if ! cargo nextest --version >/dev/null 2>&1; then
    echo "cargo-nextest is required: cargo install cargo-nextest --locked" >&2
    exit 1
fi

warn_large_target_dir() {
    [[ "$(uname -s)" == Darwin && -z "${CI:-}" && -z "${DW_NO_TARGET_DIR_CHECK:-}" ]] || return 0
    $default_deps_dir || return 0
    local limit="${DW_TARGET_DIR_WARN_FILES:-50000}" target deps count
    target="$(cargo metadata --format-version 1 --no-deps 2>/dev/null | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')" || target=""
    deps="$target/debug/deps"
    [[ -n "$target" && -d "$deps" ]] || return 0
    count="$(find "$deps" -maxdepth 1 -mindepth 1 2>/dev/null | head -n "$((limit + 1))" | wc -l)" || true
    if ((count > limit)); then
        echo "test rust: $deps holds more than $limit files, which slows every test that builds an HTTP client." >&2
        echo "  Run 'cargo clean' (or 'cargo sweep') to restore test speed." >&2
    fi
}
warn_large_target_dir

if $watch; then
    if ! command -v cargo-watch >/dev/null 2>&1; then
        echo "cargo-watch is required: cargo install cargo-watch --locked" >&2
        exit 1
    fi
    command=(./scripts/test-rust.sh)
    if $coverage; then command+=(--coverage); fi
    if ! $doctests; then command+=(--no-doctests); fi
    command+=(${args[@]+"${args[@]}"})
    printf -v watch_command '%q ' "${command[@]}"
    exec cargo watch -s "$watch_command"
fi

offline_defaulted=false
if [[ -z "${SQLX_OFFLINE+set}" ]]; then
    export SQLX_OFFLINE=true
    offline_defaulted=true
fi

defaults=()
if $default_workspace; then defaults+=(--workspace); fi
if $default_all_features; then defaults+=(--all-features); fi

status=0
if $coverage; then
    cargo llvm-cov nextest ${defaults[@]+"${defaults[@]}"} \
        --fail-under-lines 60 --lcov --output-path lcov.info ${args[@]+"${args[@]}"} || status=$?
else
    cargo nextest run ${defaults[@]+"${defaults[@]}"} ${args[@]+"${args[@]}"} || status=$?
fi

if $offline_defaulted && ((status == 101)); then
    echo "test rust: the build ran with SQLX_OFFLINE=true. If it reports 'no cached data for this query'," >&2
    echo "  run ./scripts/sqlx-prepare.sh against a migrated database, or set SQLX_OFFLINE=false." >&2
fi

if ! $no_run && $doctests; then
    cargo test --doc ${defaults[@]+"${defaults[@]}"} ${doc_args[@]+"${doc_args[@]}"} || status=$?
fi
exit "$status"
