#!/usr/bin/env bash
set -eo pipefail

# Verify a prod release landed: every deployable target's live Lambda runs the
# given version's image and settled Active/Successful. Called at the end of
# release.sh, and runnable standalone to audit prod at any time (e.g. to confirm
# a half-applied release was fully reconciled).
#
# Usage: scripts/prod/verify.sh <version> [<target>...]
#        (no targets given → verify the whole registry catalog)

UTIL="$(cd "$(dirname "$0")/../util" && pwd)"
source "$UTIL/load_env.sh"
source "$UTIL/registry.sh"

: "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
: "${AWS_REGION:?AWS_REGION not set}"
: "${AWS_PROFILE:?AWS_PROFILE not set}"

RELEASE_VERSION="$1"
shift || true
if [ -z "$RELEASE_VERSION" ]; then
    echo "Usage: scripts/prod/verify.sh <version> [<target>...]" >&2
    exit 1
fi

# No docker needed to verify — derive the registry host directly (matches
# ecr_login.sh) rather than logging in.
export REGISTRY="${REGISTRY:-${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com}"

if [ "$#" -eq 0 ]; then
    while IFS= read -r name; do
        [ -n "$name" ] && set -- "$@" "$name"
    done < <(registry_query catalog | cut -f1)
fi

export ENV="prod"
export TAG="$RELEASE_VERSION"

source "$UTIL/_verify.sh"

echo "Verifying prod release $RELEASE_VERSION ..."
verify_targets "$@"
echo "All deployable targets verified at $RELEASE_VERSION."
