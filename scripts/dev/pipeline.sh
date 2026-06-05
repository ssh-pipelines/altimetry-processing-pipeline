#!/usr/bin/env bash
set -eo pipefail

# Dev pipeline: build + push + deploy only the targets that changed vs main
# (or all of them with --all). The set of targets and the change-impact
# analysis both come from the Target registry (utilities/targets.py), which
# knows the real dependency edges — a change to utilities/, setup.py, or
# pipeline_runtime/ dirties the stages that depend on them, not just the dir
# that changed.
#
# Usage: scripts/dev/pipeline.sh [--all] [--dry-run] [--base <ref>]

# Ensure we are in the repo root
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ] || [ "$(pwd)" != "$REPO_ROOT" ]; then
    echo "Error: Must run from the repo root: $REPO_ROOT"
    exit 1
fi

DEV="$(cd "$(dirname "$0")" && pwd)"
UTIL="$DEV/../util"
source "$UTIL/registry.sh"

FORCE_ALL=false
BASE_REF="main"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --all)     FORCE_ALL=true; shift ;;
        --dry-run) export DRY_RUN=true; shift ;;
        --base)    BASE_REF="$2"; shift 2 ;;
        *)         echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

source "$UTIL/load_env.sh"

# Login to ECR once (shared with the build/deploy children via $REGISTRY).
export REGISTRY=$("$UTIL/ecr_login.sh")

# Determine which targets to (re)build/deploy via the Target registry.
TARGETS=()
if [ "$FORCE_ALL" = true ]; then
    while IFS= read -r name; do
        [ -n "$name" ] && TARGETS+=("$name")
    done < <(registry_query catalog | cut -f1)
else
    while IFS= read -r name; do
        [ -n "$name" ] && TARGETS+=("$name")
    done < <(registry_query dirty --base "$BASE_REF")
fi

if [ ${#TARGETS[@]} -eq 0 ]; then
    echo "No targets changed vs $BASE_REF; nothing to build or deploy."
    exit 0
fi

echo "Targets to build/deploy: ${TARGETS[*]}"

# build_and_push builds the container targets (and skips zip); deploy handles
# every deployable target, packaging zip targets on the fly.
"$DEV/build_and_push.sh" "${TARGETS[@]}"
"$DEV/deploy.sh" "${TARGETS[@]}"

echo "Pipeline complete for: ${TARGETS[*]}"
