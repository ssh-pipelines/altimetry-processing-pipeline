#!/usr/bin/env bash
set -eo pipefail

# Prod wrapper for the shared build/push logic. Gates on:
#   1. Working tree is clean (no uncommitted or staged changes).
#   2. HEAD is at an annotated git tag matching v<version>.
#   3. Interactive confirmation (skippable with PROD_CONFIRM=prod for CI).
#
# Usage:
#   scripts/prod/build_and_push.sh <version> <image> [<image>...]
#
# Example (deploying release 1.4.0):
#   git tag -a v1.4.0 -m "Release 1.4.0"
#   scripts/prod/build_and_push.sh 1.4.0 pipeline_runtime daily_files oer ...

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"

if [ -z "$REGISTRY" ]; then
  echo "REGISTRY not set, assuming manual run"
  echo "Logging in to ECR..."
  export REGISTRY=$("$UTIL/ecr_login.sh")
fi

RELEASE_VERSION="$1"
shift || true

if [ -z "$RELEASE_VERSION" ] || [ "$#" -eq 0 ]; then
  echo "Usage: scripts/prod/build_and_push.sh <version> <image>..."
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

# Guard 1: clean working tree.
if ! git diff --quiet HEAD -- 2>/dev/null; then
  echo "Error: working tree has uncommitted changes."
  echo "       Commit or stash them before deploying to prod."
  exit 1
fi
if ! git diff --cached --quiet 2>/dev/null; then
  echo "Error: staged changes present."
  echo "       Commit or unstage them before deploying to prod."
  exit 1
fi

# Guard 2: HEAD must be at an annotated tag matching v<version>.
TAG=$(git describe --exact-match HEAD 2>/dev/null || true)
if [ -z "$TAG" ]; then
  echo "Error: HEAD is not at an annotated tag."
  echo "       Prod deploys must be from a tag. Create one with:"
  echo "         git tag -a v$RELEASE_VERSION -m 'Release $RELEASE_VERSION'"
  exit 1
fi
EXPECTED_TAG="v$RELEASE_VERSION"
if [ "$TAG" != "$EXPECTED_TAG" ]; then
  echo "Error: HEAD tag '$TAG' does not match expected '$EXPECTED_TAG'."
  exit 1
fi

# Guard 3: interactive confirmation, bypassable via PROD_CONFIRM=prod (for CI).
if [ "${PROD_CONFIRM:-}" != "prod" ]; then
  echo
  echo "About to deploy to PROD:"
  echo "  Version: $RELEASE_VERSION"
  echo "  Tag:     $TAG"
  echo "  Images:  $*"
  echo
  read -r -p "Type 'prod' to confirm: " CONFIRM
  if [ "$CONFIRM" != "prod" ]; then
    echo "Confirmation failed; aborting."
    exit 1
  fi
fi

export ENV="prod"
export RELEASE_VERSION

exec "$UTIL/_build_and_push.sh" "$@"
