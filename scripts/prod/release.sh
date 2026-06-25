#!/usr/bin/env bash
set -eo pipefail

# Production release: build + push + deploy every target at an explicit version,
# then render + deploy the Step Functions state machine definitions. The full
# set of Lambda targets comes from the Target registry (utilities/targets.py);
# the state machines come from state_machines/*.asl.json. Bundling both here is
# deliberate — an ASL change can't ship without the Lambda changes it
# orchestrates (or vice versa), which is exactly how they drift otherwise.
#
# Usage: scripts/prod/release.sh --version <RELEASE_VERSION> [--no-cleanup] [--dry-run]

# Ensure we are in the repo root
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ] || [ "$(pwd)" != "$REPO_ROOT" ]; then
    echo "Error: Must run from the repo root: $REPO_ROOT"
    exit 1
fi

PROD="$(cd "$(dirname "$0")" && pwd)"
UTIL="$PROD/../util"
source "$UTIL/registry.sh"
source "$UTIL/load_env.sh"

# -----------------------------
# Parse flags
# -----------------------------
NO_CLEANUP=false
RELEASE_VERSION=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-cleanup) NO_CLEANUP=true; shift ;;
        --version)    RELEASE_VERSION="$2"; shift 2 ;;
        --dry-run)    export DRY_RUN=1; shift ;;
        *)            echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [ -z "$RELEASE_VERSION" ]; then
    echo "Error: --version X.Y.Z is required"
    exit 1
fi

# -----------------------------
# Discover all targets (registry is the source of truth)
# -----------------------------
ALL_TARGETS=()
while IFS= read -r name; do
    [ -n "$name" ] && ALL_TARGETS+=("$name")
done < <(registry_query catalog | cut -f1)

if [ ${#ALL_TARGETS[@]} -eq 0 ]; then
    echo "No targets found in the registry!"
    exit 1
fi

# Container targets only, for local image cleanup at the end.
CONTAINER_TARGETS=()
while IFS= read -r name; do
    [ -n "$name" ] && CONTAINER_TARGETS+=("$name")
done < <(registry_query catalog | awk -F'\t' '$3=="container"{print $1}')

# Log in to ECR once
export REGISTRY=$("$UTIL/ecr_login.sh")

# -----------------------------
# Build/push all container targets, deploy all deployable targets
# (build_and_push skips zip targets; deploy skips non-deployable ones)
# -----------------------------
"$PROD/build_and_push.sh" "$RELEASE_VERSION" "${ALL_TARGETS[@]}"
"$PROD/deploy.sh" "$RELEASE_VERSION" "${ALL_TARGETS[@]}"

# -----------------------------
# Verify every Lambda landed (live image == this version, settled Active) before
# touching the state machines, so a silent/partial Lambda deploy fails loudly
# here. Skipped in dry-run.
# -----------------------------
if [ -z "$DRY_RUN" ]; then
    "$PROD/verify.sh" "$RELEASE_VERSION" "${ALL_TARGETS[@]}"
fi

# -----------------------------
# Render + deploy the state machine definitions (after the Lambdas, so the new
# orchestration points at code that already exists). These scripts are stage-
# (not version-) based: they render from the working tree, which the prod gate
# in build_and_push.sh has already pinned to the v$RELEASE_VERSION tag. DRY_RUN
# maps to the state-machine deploy's own --dry-run flag.
# -----------------------------
SM="$PROD/../state_machines"
"$SM/render.sh" --stage prod
if [ -n "$DRY_RUN" ]; then
    "$SM/deploy.sh" --stage prod --version "$RELEASE_VERSION" --dry-run
else
    "$SM/deploy.sh" --stage prod --version "$RELEASE_VERSION"
fi

# -----------------------------
# Optional cleanup
# -----------------------------
if [ "$NO_CLEANUP" = true ] || [ -n "$DRY_RUN" ]; then
    echo "Skipping cleanup (flag: --no-cleanup or DRY_RUN mode)"
else
    echo "Cleaning up local prod images..."
    for IMAGE in "${CONTAINER_TARGETS[@]}"; do
        docker rmi "$REGISTRY/prod/$IMAGE:$RELEASE_VERSION" || true
    done

    echo "Cleaning up local dev images..."
    for IMAGE in "${CONTAINER_TARGETS[@]}"; do
        # Remove *all* dev-tagged images for this image
        docker images "$REGISTRY/dev/$IMAGE" -q | xargs -r docker rmi || true
    done
fi

echo "Production release complete: version $RELEASE_VERSION"
