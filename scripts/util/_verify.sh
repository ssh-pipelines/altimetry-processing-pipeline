#!/usr/bin/env bash
set -eo pipefail

# Shared post-deploy verification core. The wrappers set ENV, TAG, and (for
# container targets) REGISTRY, then source this file and call:
#   verify_targets <name>...
#
# For each DEPLOYABLE target it asserts the live Lambda:
#   - settled cleanly         (State=Active, LastUpdateStatus=Successful)
#   - runs the shipped artifact (container: live image URI == REGISTRY/<repo>:TAG)
# Catches the silent / partial deploy a fire-and-forget update-function-code
# can't (e.g. an aborted release that left half the functions on the old version).
# Returns non-zero if any target fails.

: "${ENV:?ENV must be set by the wrapper}"
: "${TAG:?TAG must be set by the wrapper}"

_VERIFY_UTIL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_VERIFY_UTIL/registry.sh"

# _vfield <catalog> <name> <col>  (cols: 1=name 2=path 3=packaging 4=heavy
#                                   5=deployable 6=ecr_repo 7=function)
_vfield() { awk -F'\t' -v n="$2" -v c="$3" '$1==n{print $c}' "$1"; }

_lambda_field() {  # <fn> <jmespath>
  aws --profile "$AWS_PROFILE" lambda get-function-configuration \
    --function-name "$1" --query "$2" --output text 2>/dev/null || echo "MISSING"
}

verify_targets() {
  if [ "$#" -eq 0 ]; then
    echo "verify_targets: no targets given" >&2
    return 1
  fi

  local catalog
  catalog="$(mktemp)"
  registry_query catalog --stage "$ENV" > "$catalog"

  local name pkg deployable fn ecr status state uri want rc=0
  for name in "$@"; do
    pkg="$(_vfield "$catalog" "$name" 3)"
    if [ -z "$pkg" ]; then
      echo "  ✗ unknown target '$name' (not in the Target registry)" >&2
      rc=1; continue
    fi

    deployable="$(_vfield "$catalog" "$name" 5)"
    [ "$deployable" != "true" ] && continue   # base image / stage it doesn't own

    fn="$(_vfield "$catalog" "$name" 7)"
    status="$(_lambda_field "$fn" 'LastUpdateStatus')"
    state="$(_lambda_field "$fn" 'State')"

    if [ "$status" != "Successful" ] || [ "$state" != "Active" ]; then
      echo "  ✗ $fn: State=$state LastUpdateStatus=$status" >&2
      rc=1; continue
    fi

    if [ "$pkg" = "container" ]; then
      ecr="$(_vfield "$catalog" "$name" 6)"
      want="$REGISTRY/$ecr:$TAG"
      uri="$(aws --profile "$AWS_PROFILE" lambda get-function \
        --function-name "$fn" --query 'Code.ImageUri' --output text 2>/dev/null || echo MISSING)"
      if [ "$uri" != "$want" ]; then
        echo "  ✗ $fn: live image '$uri' != expected '$want'" >&2
        rc=1; continue
      fi
    fi

    echo "  ✓ $fn ($pkg)"
  done

  rm -f "$catalog"
  return "$rc"
}
