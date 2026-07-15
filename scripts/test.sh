#!/usr/bin/env bash
# Per-stage test runner.
#
# Each stage deploys as its own isolated Lambda artifact: its Dockerfile copies
# only that stage's code and sets PYTHONPATH to the stage root, so tests import
# their modules as top-level names (`from app import ...`, `from oer.oer ...`).
# No single sys.path can host every stage at once — same-named packages (`app`,
# `config`, the per-stage `tests` package) collide. So we do NOT run one
# root-level `pytest`; we run one pytest process per stage, each rooted at that
# stage's directory. This mirrors the container import contract exactly.
#
# The stage list comes from the Target registry (utilities/targets.py) — the
# same source of truth the build/deploy scripts use — so new stages are picked
# up automatically. Only stages that actually have tests/ are run.
#
# Usage:
#   scripts/test.sh                 # run every stage that has tests
#   scripts/test.sh oer xover       # run only the named stages
#   scripts/test.sh -- -k foo -x    # pass args after `--` straight to pytest
#   scripts/test.sh oer -- -vv      # combine: named stage + pytest args
#
# Requires the dev venv (uv sync --extra dev). Uses `uv run` so the repo's
# locked interpreter/deps are used regardless of the caller's active env.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Split args into stage selectors (before `--`) and pytest passthrough (after).
selectors=()
pytest_args=()
seen_ddash=0
for arg in "$@"; do
    if [[ "$seen_ddash" -eq 1 ]]; then
        pytest_args+=("$arg")
    elif [[ "$arg" == "--" ]]; then
        seen_ddash=1
    else
        selectors+=("$arg")
    fi
done

# name<TAB>path lines from the registry catalog.
catalog="$(uv run python -m utilities.targets catalog)"

# Collect "name path" pairs for stages that have a non-empty tests/ dir,
# optionally filtered to the requested selectors.
stages=()
while IFS=$'\t' read -r name path _rest; do
    [[ -z "$name" ]] && continue
    # Skip if a selector list was given and this stage isn't in it.
    if [[ "${#selectors[@]}" -gt 0 ]]; then
        match=0
        for s in "${selectors[@]}"; do [[ "$s" == "$name" ]] && match=1; done
        [[ "$match" -eq 0 ]] && continue
    fi
    # Only run stages that actually have tests.
    if compgen -G "$path/tests/test_*.py" >/dev/null; then
        stages+=("$name|$path")
    fi
done <<< "$catalog"

# utilities/ is a real installable package (not a stage in the registry) but
# has its own test suite — include it unless a selector list excludes it.
if [[ "${#selectors[@]}" -eq 0 ]] || printf '%s\n' "${selectors[@]}" | grep -qx "utilities"; then
    if compgen -G "utilities/tests/test_*.py" >/dev/null; then
        stages+=("utilities|utilities")
    fi
fi

if [[ "${#stages[@]}" -eq 0 ]]; then
    echo "No matching stages with tests found." >&2
    exit 1
fi

passed=()
failed=()
for entry in "${stages[@]}"; do
    name="${entry%%|*}"
    path="${entry#*|}"
    echo
    echo "=============================================================="
    echo "  $name  ($path)"
    echo "=============================================================="
    # Run from the stage dir so the stage root is pytest's rootdir and the
    # stage's own packages resolve as top-level imports, exactly like the
    # container. `uv run --project` keeps the repo venv regardless of cwd.
    if (cd "$path" && uv run --project "$REPO_ROOT" pytest tests/ ${pytest_args[@]+"${pytest_args[@]}"}); then
        passed+=("$name")
    else
        failed+=("$name")
    fi
done

echo
echo "=============================================================="
echo "  Summary"
echo "=============================================================="
for n in "${passed[@]:-}"; do [[ -n "$n" ]] && echo "  PASS  $n"; done
for n in "${failed[@]:-}"; do [[ -n "$n" ]] && echo "  FAIL  $n"; done

[[ "${#failed[@]}" -eq 0 ]]
