#!/usr/bin/env bash
set -eo pipefail

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

# Fail in real mode if env not set
if [ -z "$DRY_RUN" ]; then
    : "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
    : "${AWS_REGION:?AWS_REGION not set}"
    : "${AWS_PROFILE:?AWS_PROFILE not set}"
fi

IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
    echo "No images provided to deploy.sh"
    exit 1
fi

TAG="dev-${GIT_SHA}"

for IMAGE in "${IMAGES[@]}"; do
    FULL="$REGISTRY/dev/$IMAGE:$TAG"

    if [ -z "$DRY_RUN" ]; then
        aws --profile "$AWS_PROFILE" lambda update-function-code \
            --function-name "dev-${IMAGE}" \
            --image-uri "$FULL"
        echo "Deployed dev image: $FULL"
    else
        echo "[DRY-RUN] Would deploy dev image: $FULL to function ${IMAGE}-dev"
    fi
done
