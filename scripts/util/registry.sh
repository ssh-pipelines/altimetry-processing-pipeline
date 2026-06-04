#!/usr/bin/env bash
# Thin wrapper around the Target registry (utilities/targets.py) — the single
# source of truth for what the build/deploy scripts manage. Source this file
# and call `registry_query <subcommand> [args...]`, e.g.
#
#   registry_query catalog --stage dev   # TSV: name path packaging heavy deployable ecr_repo function
#   registry_query dirty --base main     # names of targets changed vs a git ref
#
# Requires the `utilities` package importable by the chosen interpreter
# (`pip install .` from the repo root). If `python3` on PATH does not have it,
# point at a venv via PYTHON or REGISTRY_PY, e.g. REGISTRY_PY=./.venv/bin/python.

: "${REGISTRY_PY:=${PYTHON:-python3}}"

registry_query() {
    "$REGISTRY_PY" -m utilities.targets "$@"
}
