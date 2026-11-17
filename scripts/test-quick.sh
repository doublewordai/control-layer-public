#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

print=false
stdin=false
base=origin/main
args=()
while (($#)); do
    case "$1" in
        --print) print=true; shift ;;
        --stdin) stdin=true; shift ;;
        --base)
            if (($# < 2)); then echo "--base requires a value" >&2; exit 2; fi
            base="$2"; shift 2 ;;
        --base=*) base="${1#*=}"; shift ;;
        --) args+=("$@"); break ;;
        *) args+=("$1"); shift ;;
    esac
done

changed_files() {
    if $stdin; then
        cat
        return
    fi
    local merge_base
    if ! merge_base=$(git merge-base "$base" HEAD 2>/dev/null); then
        echo "test quick: no merge base with '$base'; fetch it or pass --base <ref>" >&2
        return 2
    fi
    git -c core.quotePath=false diff --name-only --no-renames "$merge_base" --
    git -c core.quotePath=false ls-files --others --exclude-standard
}

full=""
dwctl_all=false
dwctl_parts=""
parts=""
memory=false

dwctl_test() { dwctl_parts+="test(/^$1::/)"$'\n'; }
is_module() { [[ "$1" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; }
stem() { local first="${1%%/*}"; printf '%s' "${first%%.*}"; }

map_dwctl_src() {
    local rel="$1" layer rest kind name
    case "$rel" in
        lib.rs|main.rs|bin/*|test/mod.rs|test/utils.rs|test/databases.rs|test/template.rs)
            dwctl_all=true ;;
        api/handlers/*|api/models/*|db/handlers/*|db/models/*)
            layer="${rel%%/*}"; rest="${rel#*/}"; kind="${rest%%/*}"; name=$(stem "${rest#*/}")
            if [[ "$name" == mod || "$name" == repository ]]; then
                dwctl_test "$layer::$kind"
            elif is_module "$name"; then
                dwctl_test "api::handlers::$name"
                dwctl_test "api::models::$name"
                if [[ "$layer" == db ]]; then
                    dwctl_test "db::handlers::$name"
                    dwctl_test "db::models::$name"
                fi
            else
                dwctl_all=true
            fi ;;
        api/*|db/*|test/*)
            layer="${rel%%/*}"; name=$(stem "${rel#*/}")
            if [[ "$name" == mod ]]; then dwctl_test "$layer"
            elif is_module "$name"; then dwctl_test "$layer::$name"
            else dwctl_all=true; fi ;;
        *.rs|*/*)
            name=$(stem "$rel")
            if is_module "$name"; then dwctl_test "$name"; else dwctl_all=true; fi ;;
        *) dwctl_all=true ;;
    esac
}

map_path() {
    local path="$1" name
    case "$path" in
        Cargo.toml|Cargo.lock|.config/*|.cargo/*|rust-toolchain|rust-toolchain.toml)
            [[ -n "$full" ]] || full="$path" ;;
        *.md|*/LICENSE|*/Dockerfile|onwards/docs/*) ;;
        dwctl/test-macros/*)
            dwctl_all=true; parts+="package(dwctl-test-macros)"$'\n' ;;
        dwctl/src/*) map_dwctl_src "${path#dwctl/src/}" ;;
        dwctl/migrations/*|dwctl/underway-migrations/*)
            dwctl_parts+="kind(test)"$'\n'; dwctl_test migrations ;;
        dwctl/tests/memory_budget/*) memory=true ;;
        dwctl/tests/*)
            name="${path#dwctl/tests/}"
            if [[ "$name" == *.rs && "$name" != */* && -f "$path" && "${name%.rs}" =~ ^[A-Za-z0-9_-]+$ ]]; then
                dwctl_parts+="binary(${name%.rs})"$'\n'
            else
                dwctl_parts+="kind(test)"$'\n'
                dwctl_test migrations
            fi ;;
        dwctl/*) dwctl_all=true ;;
        fusillade-arsenal/migrations/*)
            parts+="package(fusillade-arsenal)"$'\n'; dwctl_test migrations ;;
        fusillade/*|fusillade-arsenal/*|fusillade-core/*|onwards/*|openai-reassembler/*)
            parts+="package(${path%%/*})"$'\n' ;;
        crates/dwctl-stripe/*)
            parts+="package(dwctl-stripe)"$'\n'; dwctl_test payment_providers ;;
        crates/*/*)
            name="${path#crates/}"; parts+="package(${name%%/*})"$'\n' ;;
    esac
}

files=$(changed_files)
while IFS= read -r path; do
    [[ -n "$path" ]] && map_path "$path"
done <<<"$files"

unique() { printf '%s' "$1" | LC_ALL=C sort -u; }
join_or() {
    local line out=""
    while IFS= read -r line; do
        [[ -n "$line" ]] && out+="${out:+ | }$line"
    done
    printf '%s' "$out"
}

if $dwctl_all; then
    parts+="package(dwctl)"$'\n'
elif [[ -n "$dwctl_parts" ]]; then
    inner=$(unique "$dwctl_parts" | join_or)
    [[ "$inner" == *" | "* ]] && inner="($inner)"
    parts+="package(dwctl) & $inner"$'\n'
fi

if [[ -n "$full" ]]; then
    expr="all()"
    echo "test quick: $full changed; running the whole workspace" >&2
else
    expr=$(unique "$parts" | join_or)
fi

if $memory; then
    echo "test quick: memory-budget tests changed; run them with 'just test memory'" >&2
fi

if [[ -z "$expr" ]]; then
    echo "test quick: no Rust changes against $base; nothing to run" >&2
    if $print; then echo "none()"; fi
    exit 0
fi

if $print; then
    echo "$expr"
    exit 0
fi

echo "test quick: -E '$expr'" >&2
filter=()
if [[ -z "$full" ]]; then filter=(-E "$expr"); fi
exec ./scripts/test-rust.sh --no-doctests ${filter[@]+"${filter[@]}"} --no-tests=warn ${args[@]+"${args[@]}"}
