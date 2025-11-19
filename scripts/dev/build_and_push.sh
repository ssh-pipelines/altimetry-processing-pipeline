#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
    echo "No images provided to build_and_push.sh"
    exit 1
fi

TAG="dev-${GIT_SHA}"

for IMAGE in "${IMAGES[@]}"; do
    DIR=$(find pipeline -type d -name "$IMAGE" | head -1)

    FULL="$REGISTRY/dev/$IMAGE:$TAG"

    if [ -z "$DRY_RUN" ]; then
        echo "Building: $FULL"
        docker build "$DIR" \
            --build-arg GIT_SHA="$GIT_SHA" \
            --build-arg BUILD_ENV="dev" \
            --build-arg BUILD_DATE="$BUILD_DATE" \
            -t "$FULL"

        echo "Pushing: $FULL"
        docker push "$FULL"
    else
        echo "[DRY-RUN] Would build: $FULL from $DIR"
        echo "[DRY-RUN] Would push: $FULL"
    fi
done
