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

# Images that are build-only artifacts (base images, etc.) — pushed to ECR for
# stage builds to consume, but with no corresponding Lambda function to update.
BUILD_ONLY_IMAGES=(pipeline_runtime)

is_build_only() {
    for s in "${BUILD_ONLY_IMAGES[@]}"; do
        [[ "$s" == "$1" ]] && return 0
    done
    return 1
}

TAG="dev-${GIT_SHA}"

for IMAGE in "${IMAGES[@]}"; do
    if is_build_only "$IMAGE"; then
        echo "Skipping deploy for $IMAGE (base image; no Lambda function)"
        continue
    fi

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
