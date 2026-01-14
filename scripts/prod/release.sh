#!/usr/bin/env bash
set -eo pipefail

# Ensure we are in the repo root
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ] || [ "$(pwd)" != "$REPO_ROOT" ]; then
    echo "Error: Must run from the repo root: $REPO_ROOT"
    exit 1
fi

PROD="$(cd "$(dirname "$0")" && pwd)"
UTIL="$PROD/../util"

source "$UTIL/load_env.sh"

# -----------------------------
# Parse flags
# -----------------------------
NO_CLEANUP=false
RELEASE_VERSION=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-cleanup)
            NO_CLEANUP=true
            shift
            ;;
        --version)
            RELEASE_VERSION="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown argument: $1"
            exit 1
            ;;
    esac
done

export DRY_RUN

if [ -z "$RELEASE_VERSION" ]; then
    echo "Error: --version X.Y.Z is required"
    exit 1
fi

# -----------------------------
# Discover all images
# -----------------------------
IMAGES=()
while IFS= read -r img; do
    IMAGES+=("$img")
done < <("$UTIL/find_images.sh")

if [ ${#IMAGES[@]} -eq 0 ]; then
    echo "No Docker images found!"
    exit 1
fi

# Log in to ECR once
export REGISTRY=$("$UTIL/ecr_login.sh")

# -----------------------------
# Build and push ALL images
# -----------------------------
"$PROD/build_and_push.sh" "$RELEASE_VERSION" "${IMAGES[@]}"

# -----------------------------
# Deploy ALL images
# -----------------------------
"$PROD/deploy.sh" "$RELEASE_VERSION" "${IMAGES[@]}"

# -----------------------------
# Optional cleanup
# -----------------------------
if [ "$NO_CLEANUP" = false ] && [ -z "$DRY_RUN" ]; then
    echo "Cleaning up local prod images..."
    for IMAGE in "${IMAGES[@]}"; do
        docker rmi "$REGISTRY/prod/$IMAGE:$RELEASE_VERSION" || true
    done

    echo "Cleaning up local dev images..."
    for IMAGE in "${IMAGES[@]}"; do
        # Remove *all* dev-tagged images for this image
        docker images "$REGISTRY/dev/$IMAGE" -q | xargs -r docker rmi || true
    done

else
    echo "Skipping cleanup (flag: --no-cleanup or DRY_RUN mode)"
fi

echo "Production release complete: version $RELEASE_VERSION"
