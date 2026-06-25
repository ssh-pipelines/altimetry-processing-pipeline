#!/usr/bin/env bash
set -eo pipefail

# Shared deploy core. The dev/prod wrappers set ENV, TAG, and REGISTRY, then
# source this file and call:  deploy_targets <name>...
#
# Deploys each DEPLOYABLE target via the Target registry, branching on packaging
# kind at the packaging seam:
#   container -> aws lambda update-function-code --image-uri <registry>/<repo>:<TAG>
#   zip       -> zip the source dir and update-function-code --zip-file
# Non-deployable targets (e.g. the pipeline_runtime base image) are skipped.

: "${ENV:?ENV must be set by the wrapper}"
: "${TAG:?TAG must be set by the wrapper}"

_DEPLOY_UTIL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$_DEPLOY_UTIL/registry.sh"

# _tfield <catalog> <name> <col>  (cols: 1=name 2=path 3=packaging 4=heavy
#                                   5=deployable 6=ecr_repo 7=function)
_tfield() { awk -F'\t' -v n="$2" -v c="$3" '$1==n{print $c}' "$1"; }

deploy_targets() {
  if [ "$#" -eq 0 ]; then
    echo "deploy_targets: no targets given" >&2
    return 1
  fi

  local repo_root catalog
  repo_root="$(git rev-parse --show-toplevel)"
  catalog="$(mktemp)"
  registry_query catalog --stage "$ENV" > "$catalog"

  local IMAGE pkg deployable fn ecr dir uri zipfile rc=0
  for IMAGE in "$@"; do
    pkg="$(_tfield "$catalog" "$IMAGE" 3)"
    if [ -z "$pkg" ]; then
      echo "Error: unknown target '$IMAGE' (not in the Target registry)" >&2
      rc=1
      break
    fi

    # Stage-aware: the registry reports a target non-deployable when this stage
    # doesn't own it — the base image (never deployed) or a shared singleton
    # restricted to another stage (e.g. podaac_cred_update is prod-only).
    deployable="$(_tfield "$catalog" "$IMAGE" 5)"
    if [ "$deployable" != "true" ]; then
      echo "Skipping $IMAGE (not deployable in $ENV)"
      continue
    fi

    fn="$(_tfield "$catalog" "$IMAGE" 7)"

    case "$pkg" in
      container)
        ecr="$(_tfield "$catalog" "$IMAGE" 6)"
        uri="$REGISTRY/$ecr:$TAG"
        if [ -z "$DRY_RUN" ]; then
          aws --profile "$AWS_PROFILE" lambda update-function-code \
            --function-name "$fn" --image-uri "$uri"
          # update-function-code is async: wait for the new image to settle so a
          # failed update (bad image, missing perms) fails the deploy here rather
          # than surfacing later. Mirrors the zip branch.
          aws --profile "$AWS_PROFILE" lambda wait function-updated-v2 \
            --function-name "$fn"
          echo "Deployed $fn <- $uri"
        else
          echo "[DRY-RUN] Would deploy $fn <- $uri"
        fi
        ;;
      zip)
        dir="$repo_root/$(_tfield "$catalog" "$IMAGE" 2)"
        zipfile="$(mktemp -u).zip"
        if [ -z "$DRY_RUN" ]; then
          ( cd "$dir" && zip -r -X "$zipfile" . \
              -x 'README.md' 'tests/*' '*/__pycache__/*' '__pycache__/*' '*.pyc' \
              >/dev/null )
          aws --profile "$AWS_PROFILE" lambda update-function-code \
            --function-name "$fn" --zip-file "fileb://$zipfile"
          aws --profile "$AWS_PROFILE" lambda wait function-updated-v2 \
            --function-name "$fn"
          aws --profile "$AWS_PROFILE" lambda update-function-configuration \
            --function-name "$fn" --handler "app.lambda_handler" \
            > /dev/null
          rm -f "$zipfile"
          echo "Deployed $fn <- $dir (zip)"
        else
          echo "[DRY-RUN] Would zip $dir and deploy $fn"
        fi
        ;;
      *)
        echo "Error: unknown packaging '$pkg' for $IMAGE" >&2
        rc=1
        break
        ;;
    esac
  done

  rm -f "$catalog"
  return "$rc"
}
