#!/usr/bin/env bash
set -eo pipefail

# Ensure we are in the repo root
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ] || [ "$(pwd)" != "$REPO_ROOT" ]; then
    echo "Error: Must run from the repo root: $REPO_ROOT"
    exit 1
fi

DEV="$(cd "$(dirname "$0")" && pwd)"
UTIL="$DEV/../util"

# Check for --all flag
FORCE_ALL=false
for arg in "$@"; do
    if [ "$arg" == "--all" ]; then
        FORCE_ALL=true
        # Remove the flag from args so it doesn't get passed down
        set -- "${@//$arg/}"
    fi
    if [ "$arg" == "--dry-run" ]; then
        export DRY_RUN=true
        # Remove the flag from args so it doesn't get passed down
        set -- "${@//$arg/}"
    fi
done

source "$UTIL/load_env.sh"

# Login to ECR once
export REGISTRY=$("$UTIL/ecr_login.sh")

# Detect all images once
ALL_IMAGES=()
while IFS= read -r img; do
    ALL_IMAGES+=("$img")
done < <("$UTIL/find_images.sh")

if [ ${#ALL_IMAGES[@]} -eq 0 ]; then
    echo "No Docker images found!"
    exit 1
fi

# Determine which images need to be built
IMAGES_TO_BUILD=()
for IMAGE in "${ALL_IMAGES[@]}"; do
    DIR=$(find pipeline -type d -name "$IMAGE" | head -1)

    if [ "$FORCE_ALL" = true ] || ! git diff --quiet main...HEAD -- "$DIR"; then
        IMAGES_TO_BUILD+=("$IMAGE")
    else
        echo "Skipping $IMAGE (no changes compared to main)"
    fi
done

if [ ${#IMAGES_TO_BUILD[@]} -eq 0 ]; then
    echo "No images need to be built or deployed."
    exit 0
fi

# Pass only the images that need to be built/deployed
"$DEV/build_and_push.sh" "${IMAGES_TO_BUILD[@]}"
"$DEV/deploy.sh" "${IMAGES_TO_BUILD[@]}"

echo "Pipeline complete for images: ${IMAGES_TO_BUILD[*]}"
