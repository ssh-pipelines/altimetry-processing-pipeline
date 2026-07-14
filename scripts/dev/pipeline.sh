#!/usr/bin/env bash
set -eo pipefail

# Dev pipeline: build + push + deploy only the targets that changed vs main
# (or all of them with --all). The set of targets and the change-impact
# analysis both come from the Target registry (utilities/targets.py), which
# knows the real dependency edges — a change to utilities/, pyproject.toml, or
# pipeline_runtime/ dirties the stages that depend on them, not just the dir
# that changed.
#
# State machines aren't Targets (the registry doesn't track them), so they're
# gated separately: if any state_machines/*.asl.json changed vs the base (or
# with --all), render + deploy them to dev after the Lambdas. This mirrors
# release.sh, which always ships both halves together.
#
# With --smoke, run the non-mutating end-to-end smoke test after deploying.
# Source/window come from the environment (SMOKE_SOURCE / SMOKE_START / SMOKE_END,
# optional SMOKE_BUCKET) so you don't retype them; set them in .env. It's opt-in
# so a routine dev push doesn't start an execution unless you ask.
#
# Usage: scripts/dev/pipeline.sh [--all] [--dry-run] [--base <ref>] [--smoke]

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
SMOKE=false
while [ "$#" -gt 0 ]; do
    case "$1" in
        --all)     FORCE_ALL=true; shift ;;
        --dry-run) export DRY_RUN=true; shift ;;
        --base)    BASE_REF="$2"; shift 2 ;;
        --smoke)   SMOKE=true; shift ;;
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

# State machines live outside the registry: --all forces them, otherwise deploy
# only when a top-level template changed vs the base. The :(glob) pathspec keeps
# `*` from crossing into rendered/ (a gitignored build artifact anyway).
SM_CHANGED=false
if [ "$FORCE_ALL" = true ]; then
    SM_CHANGED=true
elif [ -n "$(git diff --name-only "$BASE_REF"...HEAD -- ':(glob)state_machines/*.asl.json')" ]; then
    SM_CHANGED=true
fi

if [ ${#TARGETS[@]} -eq 0 ] && [ "$SM_CHANGED" = false ]; then
    echo "No targets or state machines changed vs $BASE_REF; nothing to do."
    exit 0
fi

# build_and_push builds the container targets (and skips zip); deploy handles
# every deployable target, packaging zip targets on the fly.
if [ ${#TARGETS[@]} -gt 0 ]; then
    echo "Targets to build/deploy: ${TARGETS[*]}"
    "$DEV/build_and_push.sh" "${TARGETS[@]}"
    "$DEV/deploy.sh" "${TARGETS[@]}"
else
    echo "No Lambda targets changed vs $BASE_REF."
fi

# Render + deploy state machines after the Lambdas (so new orchestration points
# at code that already exists). DRY_RUN maps to deploy.sh's own --dry-run flag.
if [ "$SM_CHANGED" = true ]; then
    echo "State machine definitions changed; rendering + deploying (stage=dev)."
    SM="$DEV/../state_machines"
    "$SM/render.sh" --stage dev
    if [ -n "$DRY_RUN" ]; then
        "$SM/deploy.sh" --stage dev --version "$GIT_SHA" --dry-run
    else
        "$SM/deploy.sh" --stage dev --version "$GIT_SHA"
    fi
else
    echo "No state machine changes vs $BASE_REF; skipping state machine deploy."
fi

# Optional post-deploy smoke (--smoke): non-mutating end-to-end check against an
# already-processed window. Params come from the environment; smoke.sh's own
# pre-check refuses windows with real work, and falls back to BUCKET_NAME when
# SMOKE_BUCKET is unset.
if [ "$SMOKE" = true ] && [ -z "$DRY_RUN" ]; then
    : "${SMOKE_SOURCE:?--smoke needs SMOKE_SOURCE set (e.g. in .env)}"
    : "${SMOKE_START:?--smoke needs SMOKE_START (YYYY-MM-DD)}"
    : "${SMOKE_END:?--smoke needs SMOKE_END (YYYY-MM-DD)}"
    smoke_args=(--stage dev --source "$SMOKE_SOURCE" --start "$SMOKE_START" --end "$SMOKE_END")
    [ -n "${SMOKE_BUCKET:-}" ] && smoke_args+=(--bucket "$SMOKE_BUCKET")
    echo "Running post-deploy smoke (source=$SMOKE_SOURCE, $SMOKE_START..$SMOKE_END)."
    "$DEV/../smoke.sh" "${smoke_args[@]}"
elif [ "$SMOKE" = true ]; then
    echo "Skipping smoke (--dry-run)."
fi

echo "Pipeline complete."
