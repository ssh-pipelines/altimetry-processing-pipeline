#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

RELEASE_VERSION="$1"
shift
IMAGES=("$@")

if [ -z "$RELEASE_VERSION" ]; then
    echo "build_and_push.sh requires: <version> <image>..."
    exit 1
fi

for IMAGE in "${IMAGES[@]}"; do
    DIR=$(find pipeline -type d -name "$IMAGE" | head -1)
    FULL="$REGISTRY/prod/$IMAGE:$RELEASE_VERSION"

    if [ -z "$DRY_RUN" ]; then
        echo "Building: $FULL"
        docker build "$DIR" \
            --build-arg BUILD_ENV="prod" \
            --build-arg BUILD_DATE="$BUILD_DATE" \
            --build-arg RELEASE_VERSION="$RELEASE_VERSION" \
            -t "$FULL"

        echo "Pushing: $FULL"
        docker push "$FULL"
    else
        echo "[DRY-RUN] Would build: $FULL from $DIR"
        echo "[DRY-RUN] Would push: $FULL"
    fi
done
