#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
    echo "No images provided to deploy.sh"
    exit 1
fi

TAG="dev-${GIT_SHA}"

for IMAGE in "${IMAGES[@]}"; do
    FULL="$REGISTRY/dev/$IMAGE:$TAG"

    if [ -z "$DRY_RUN" ]; then
        aws lambda update-function-code \
            --function-name "${IMAGE}-dev" \
            --image-uri "$FULL"
        echo "Deployed dev image: $FULL"
    else
        echo "[DRY-RUN] Would deploy dev image: $FULL"
    fi
done
