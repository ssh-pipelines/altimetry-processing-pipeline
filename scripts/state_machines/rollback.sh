#!/usr/bin/env bash
set -eo pipefail

# Roll the state machines back (or forward) to a released version.
#
# Unlike Lambda images — which persist per-version in ECR, so a rollback is just
# `update-function-code --image-uri prod/<x>:<old>` — Step Functions overwrites
# its definition in place, leaving no per-version artifact. The annotated git tag
# vX.Y.Z is therefore the version-keyed source of truth: rendering that tag's
# templates reproduces, byte-for-byte, what the release deployed (render is a pure
# ACCOUNT/REGION/STAGE substitution).
#
# Mechanism: shallow-clone the tag into a tmp dir (your working tree is never
# touched — safe to run mid-incident from a dirty checkout), render that tag's
# definitions, then deploy them with the CURRENT deploy.sh. We deliberately keep
# the *definitions* pinned to the tag but the *tooling* current, so rolling back
# to an old release can't drag along that release's deploy bugs (e.g. the
# prod-podaac_auth crash fixed after 2.1.0).
#
# Scope: this restores the definitions present in the target tag. Machines added
# since are left untouched; it does not delete resources.
#
# Usage:
#   scripts/state_machines/rollback.sh --version <X.Y.Z|vX.Y.Z> [--stage dev|prod]
#                                      [--file <name>.asl.json] [--dry-run]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load .env / AWS_* from the *current* checkout; the clone has no .env, so its
# render.sh relies on these being exported into the environment it inherits.
source "$REPO_ROOT/scripts/util/load_env.sh"

# ── Defaults ────────────────────────────────────────────────────────
STAGE="dev"
VERSION=""
TARGET_FILE=""
DRY_RUN=false

# ── Parse arguments ─────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --version) VERSION="$2";     shift 2 ;;
        --stage)   STAGE="$2";       shift 2 ;;
        --file)    TARGET_FILE="$2"; shift 2 ;;
        --dry-run) DRY_RUN=true;     shift   ;;
        -h|--help)
            echo "Usage: $0 --version <X.Y.Z> [--stage dev|prod] [--file <name>.asl.json] [--dry-run]"
            echo ""
            echo "Re-deploys a released version's state machine definitions by"
            echo "rendering them from that version's git tag. Definitions come from"
            echo "the tag; the deploy uses current tooling."
            echo ""
            echo "Options:"
            echo "  --version   Release to roll to, e.g. 2.1.0 (the vX.Y.Z tag)"
            echo "  --stage     Deployment stage (default: dev)"
            echo "  --file      Roll back a single machine instead of all"
            echo "  --dry-run   Print what would be deployed without executing"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

[ -n "$VERSION" ] || { echo "Error: --version X.Y.Z is required" >&2; exit 1; }

# Normalize: TAG carries the v-prefix (clone target); VERSION_BARE is the value
# stamped as the live 'version' tag (matches release.sh's bare format).
TAG="$VERSION"; [[ "$TAG" == v* ]] || TAG="v$TAG"
VERSION_BARE="${TAG#v}"

ORIGIN="$(git -C "$REPO_ROOT" remote get-url origin)"

TMP="$(mktemp -d)"
cleanup() { rm -rf "$TMP"; }
trap cleanup EXIT

echo "Rolling $STAGE state machines to $TAG"
echo "  cloning $TAG from $ORIGIN ..."
if ! git clone --quiet --depth 1 --branch "$TAG" "$ORIGIN" "$TMP/repo"; then
    echo "Error: could not clone tag $TAG — does it exist on origin?" >&2
    exit 1
fi

# Render the tag's definitions (its own render.sh, so the substitution matches
# how that version rendered). AWS_ACCOUNT_ID / AWS_REGION are inherited.
render_args=(--stage "$STAGE")
[ -n "$TARGET_FILE" ] && render_args+=(--file "$TARGET_FILE")
"$TMP/repo/scripts/state_machines/render.sh" "${render_args[@]}"

# Hand the tag-pinned definitions to the CURRENT deploy tooling. rendered/ is a
# gitignored build-artifact dir, so refreshing it here is within its contract.
mkdir -p "$REPO_ROOT/state_machines/rendered"
cp "$TMP/repo/state_machines/rendered/"*.asl.json "$REPO_ROOT/state_machines/rendered/"

deploy_args=(--stage "$STAGE" --version "$VERSION_BARE")
[ -n "$TARGET_FILE" ] && deploy_args+=(--file "$TARGET_FILE")
[ "$DRY_RUN" = true ] && deploy_args+=(--dry-run)
"$REPO_ROOT/scripts/state_machines/deploy.sh" "${deploy_args[@]}"

if [ "$DRY_RUN" = true ]; then
    echo "Dry run complete — nothing changed. Re-run without --dry-run to apply $TAG."
else
    echo "Rolled $STAGE state machines to $TAG."
fi
