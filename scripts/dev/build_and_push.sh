#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

if [ -z "$REGISTRY" ]; then
    echo "REGISTRY not set, assuming manual run"
    echo "Logging in to ECR..."
    DEV="$(cd "$(dirname "$0")" && pwd)"
    UTIL="$DEV/../util"

    export REGISTRY=$("$UTIL/ecr_login.sh")
fi

IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
    echo "No images provided to build_and_push.sh"
    exit 1
fi

TAG="dev-${GIT_SHA}"

REPO_ROOT="$(git rev-parse --show-toplevel)"

for IMAGE in "${IMAGES[@]}"; do
    DIR=$(find pipeline -type d -name "$IMAGE" | head -1)

    FULL="$REGISTRY/dev/$IMAGE:$TAG"

    if [ -z "$DRY_RUN" ]; then
        echo "Building: $FULL"
        docker buildx build --platform linux/amd64 \
            -f "$DIR/Dockerfile" \
            "$REPO_ROOT" \
            --build-arg GIT_SHA="$GIT_SHA" \
            --build-arg BUILD_ENV="dev" \
            --build-arg BUILD_DATE="$BUILD_DATE" \
            --load -t "$FULL"

        echo "Pushing: $FULL"
        docker push "$FULL"
    else
        echo "[DRY-RUN] Would build: $FULL from $DIR"
        echo "[DRY-RUN] Would push: $FULL"
    fi
done
