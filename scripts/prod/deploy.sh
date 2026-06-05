#!/usr/bin/env bash
set -eo pipefail

# Prod deploy wrapper. Deploys the given targets at the release-version tag via
# the shared deploy core, which queries the Target registry and branches on
# packaging kind (container image vs zip).
#
# Usage: scripts/prod/deploy.sh <version> <target> [<target>...]

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

# Fail in real mode if env not set
if [ -z "$DRY_RUN" ]; then
    : "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
    : "${AWS_REGION:?AWS_REGION not set}"
    : "${AWS_PROFILE:?AWS_PROFILE not set}"
fi

RELEASE_VERSION="$1"
shift || true

if [ -z "$RELEASE_VERSION" ] || [ "$#" -eq 0 ]; then
    echo "deploy.sh requires: <version> <target>..."
    exit 1
fi

if [ -z "$REGISTRY" ]; then
    echo "REGISTRY not set, logging in to ECR..." >&2
    export REGISTRY=$("$UTIL/ecr_login.sh")
fi

export ENV="prod"
export TAG="$RELEASE_VERSION"

source "$UTIL/_deploy.sh"
deploy_targets "$@"
