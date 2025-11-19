#!/usr/bin/env bash
set -eo pipefail

# Load .env file if it exists
ENV_FILE="$(dirname "$0")/../../.env"

if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

# Optional: set some defaults
export BRANCH=${BRANCH:-$(git rev-parse --abbrev-ref HEAD)}
export GIT_SHA=${GIT_SHA:-$(git rev-parse --short HEAD)}
export BUILD_DATE=${BUILD_DATE:-$(date -u +"%Y-%m-%dT%H:%M:%SZ")}