#!/usr/bin/env bash
set -eo pipefail

# Dev deploy wrapper. Deploys the given targets at the current git-SHA tag via
# the shared deploy core, which queries the Target registry and branches on
# packaging kind (container image vs zip).
#
# Usage: scripts/dev/deploy.sh <target> [<target>...]

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

if [ -z "$REGISTRY" ]; then
    echo "REGISTRY not set, logging in to ECR..." >&2
    export REGISTRY=$("$UTIL/ecr_login.sh")
fi

# Fail in real mode if env not set
if [ -z "$DRY_RUN" ]; then
    : "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
    : "${AWS_REGION:?AWS_REGION not set}"
    : "${AWS_PROFILE:?AWS_PROFILE not set}"
fi

if [ "$#" -eq 0 ]; then
    echo "No targets provided to deploy.sh"
    exit 1
fi

export ENV="dev"
export TAG="dev-${GIT_SHA}"

source "$UTIL/_deploy.sh"
deploy_targets "$@"
