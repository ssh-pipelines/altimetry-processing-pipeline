#!/usr/bin/env bash
set -eo pipefail

# Dev wrapper for the shared build/push logic. Tags images with the current
# git short SHA (no version bookkeeping required for dev iteration).
#
# Usage:
#   scripts/dev/build_and_push.sh <image> [<image>...]
#
# Examples:
#   # Rebuild a single stage (runtime tag must already exist in ECR):
#   scripts/dev/build_and_push.sh daily_files
#
#   # Rebuild the runtime and one or more stages atomically:
#   scripts/dev/build_and_push.sh pipeline_runtime daily_files oer

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

if [ -z "$REGISTRY" ]; then
  echo "REGISTRY not set, assuming manual run"
  echo "Logging in to ECR..."
  export REGISTRY=$("$UTIL/ecr_login.sh")
fi

export ENV="dev"
export RELEASE_VERSION="dev-${GIT_SHA}"

exec "$UTIL/_build_and_push.sh" "$@"
