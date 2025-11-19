#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

RELEASE_VERSION="$1"
shift
IMAGES=("$@")

if [ -z "$RELEASE_VERSION" ]; then
    echo "deploy.sh requires: <version> <image>..."
    exit 1
fi

for IMAGE in "${IMAGES[@]}"; do
    FULL="$REGISTRY/prod/$IMAGE:$RELEASE_VERSION"

    if [ -z "$DRY_RUN" ]; then
        aws lambda update-function-code \
            --function-name "${IMAGE}-prod" \
            --image-uri "$FULL"

        echo "Deployed prod image: $FULL"
    else
        echo "[DRY-RUN] Would deploy prod image: $FULL"
    fi
done
