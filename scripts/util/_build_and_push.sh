#!/usr/bin/env bash
set -eo pipefail

# Shared build/push core. Invoked by scripts/dev/build_and_push.sh and
# scripts/prod/build_and_push.sh, which set ENV, RELEASE_VERSION, and REGISTRY
# before exec'ing into this script.
#
# Builds and pushes CONTAINER targets only — zip targets (the pipeline/infra
# Lambdas) have no image and are packaged at deploy time. Which targets exist,
# where they live, and which are heavy all come from the Target registry
# (utilities/targets.py); there are no hardcoded lists here.
#
# Responsibilities:
#   - Pass the BASE_IMAGE build arg to heavy stages so they FROM the matching
#     pipeline_runtime tag.
#   - Order pipeline_runtime first when it's part of the same invocation, so
#     stages can FROM it.
#   - When pipeline_runtime is not in this invocation, verify the expected tag
#     exists in ECR before letting any heavy stage build proceed.

: "${ENV:?ENV must be set by the wrapper (dev or prod)}"
: "${RELEASE_VERSION:?RELEASE_VERSION must be set by the wrapper}"
: "${REGISTRY:?REGISTRY must be set by the wrapper}"

UTIL="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$UTIL/registry.sh"

IMAGES=("$@")
if [ ${#IMAGES[@]} -eq 0 ]; then
  echo "Error: no images specified"
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"

# Snapshot the catalog once; look fields up with awk (bash 3.2-safe — no
# associative arrays). Columns: 1=name 2=path 3=packaging 4=heavy 5=deployable.
CATALOG="$(mktemp)"
trap 'rm -f "$CATALOG"' EXIT
registry_query catalog > "$CATALOG"

tfield()       { awk -F'\t' -v n="$1" -v c="$2" '$1==n{print $c}' "$CATALOG"; }
is_heavy()     { [ "$(tfield "$1" 4)" = "true" ]; }
packaging_of() { tfield "$1" 3; }
path_of()      { tfield "$1" 2; }

# Sort: pipeline_runtime first if requested, so its tag exists in the local
# Docker cache before any stage tries to FROM it.
SORTED=()
runtime_in_list=0
for IMAGE in "${IMAGES[@]}"; do
  if [ "$IMAGE" = "pipeline_runtime" ]; then
    SORTED+=("$IMAGE")
    runtime_in_list=1
  fi
done
for IMAGE in "${IMAGES[@]}"; do
  [ "$IMAGE" != "pipeline_runtime" ] && SORTED+=("$IMAGE")
done

# Precondition: heavy stages need a matching pipeline_runtime tag to FROM. If
# the runtime isn't being (re)built in this invocation, verify the tag exists.
if [ "$runtime_in_list" -eq 0 ]; then
  for IMAGE in "${IMAGES[@]}"; do
    if is_heavy "$IMAGE"; then
      RUNTIME_REPO="$ENV/pipeline_runtime"
      if ! aws ecr describe-images \
            --repository-name "$RUNTIME_REPO" \
            --image-ids "imageTag=$RELEASE_VERSION" \
            >/dev/null 2>&1; then
        echo "Error: heavy stage '$IMAGE' needs $RUNTIME_REPO:$RELEASE_VERSION,"
        echo "       but that tag was not found in ECR."
        echo "       Either include 'pipeline_runtime' in this invocation, or"
        echo "       build it first."
        exit 1
      fi
      break
    fi
  done
fi

for IMAGE in "${SORTED[@]}"; do
  PKG="$(packaging_of "$IMAGE")"
  if [ -z "$PKG" ]; then
    echo "Error: unknown target '$IMAGE' (not in the Target registry)"
    exit 1
  fi
  if [ "$PKG" != "container" ]; then
    echo "Skipping $IMAGE (packaging=$PKG; packaged at deploy time, not built)"
    continue
  fi

  DIR="$REPO_ROOT/$(path_of "$IMAGE")"
  FULL="$REGISTRY/$ENV/$IMAGE:$RELEASE_VERSION"
  REPO_NAME="$ENV/$IMAGE"

  BUILD_ARGS=(
    --build-arg "BUILD_ENV=$ENV"
    --build-arg "BUILD_DATE=${BUILD_DATE:-}"
    --build-arg "RELEASE_VERSION=$RELEASE_VERSION"
  )

  if is_heavy "$IMAGE"; then
    BUILD_ARGS+=(
      --build-arg "BASE_IMAGE=$REGISTRY/$ENV/pipeline_runtime:$RELEASE_VERSION"
    )
  fi

  if [ -z "$DRY_RUN" ]; then
    echo "Building: $FULL"
    docker buildx build --platform linux/amd64 \
      -f "$DIR/Dockerfile" \
      "$REPO_ROOT" \
      "${BUILD_ARGS[@]}" \
      --load -t "$FULL"

    echo "Ensuring ECR repository exists: $REPO_NAME"
    if ! aws ecr describe-repositories --repository-names "$REPO_NAME" >/dev/null 2>&1; then
      echo "Creating ECR repository: $REPO_NAME"
      aws ecr create-repository --repository-name "$REPO_NAME"
    fi

    echo "Pushing: $FULL"
    docker push "$FULL"
  else
    echo "[DRY-RUN] Would build: $FULL from $DIR"
    echo "[DRY-RUN] Would ensure ECR repository exists: $REPO_NAME"
    echo "[DRY-RUN] Would push: $FULL"
  fi
done
