#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Load environment ────────────────────────────────────────────────
source "$REPO_ROOT/scripts/util/load_env.sh"

# ── Defaults ────────────────────────────────────────────────────────
STAGE="dev"
TARGET_FILE=""
DRY_RUN=false

# ── Parse arguments ─────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage)    STAGE="$2";       shift 2 ;;
        --file)     TARGET_FILE="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=true;     shift   ;;
        -h|--help)
            echo "Usage: $0 [--stage dev|prod] [--file <name>.asl.json] [--dry-run]"
            echo ""
            echo "Deploys rendered state machine definitions to AWS."
            echo ""
            echo "Options:"
            echo "  --stage     Deployment stage (default: dev)"
            echo "  --file      Deploy a single rendered file instead of all"
            echo "  --dry-run   Print commands without executing"
            echo ""
            echo "Required environment variables (via .env or exported):"
            echo "  AWS_ACCOUNT_ID   AWS account number"
            echo "  AWS_REGION       AWS region (e.g. us-west-2)"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

export STAGE

# ── Validate ────────────────────────────────────────────────────────
: "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set — add it to .env or export it}"
: "${AWS_REGION:?AWS_REGION not set — add it to .env or export it}"

RENDERED_DIR="$REPO_ROOT/state_machines/rendered"

if [[ ! -d "$RENDERED_DIR" ]]; then
    echo "Error: rendered directory not found at $RENDERED_DIR" >&2
    echo "Run scripts/state_machines/render.sh first." >&2
    exit 1
fi

# ── Deploy ──────────────────────────────────────────────────────────
deploy_file() {
    local filepath="$1"
    local filename
    filename="$(basename "$filepath")"
    local base="${filename%.asl.json}"
    local sm_name="nasa-ssh-pipeline-${STAGE}-${base}-sm"
    local arn="arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:${sm_name}"

    if [[ "$DRY_RUN" == true ]]; then
        echo "  [dry-run] aws stepfunctions update-state-machine \\"
        echo "    --state-machine-arn $arn \\"
        echo "    --definition file://$filepath"
    else
        echo "  deploying $filename → $sm_name"
        aws stepfunctions update-state-machine \
            --state-machine-arn "$arn" \
            --definition "file://$filepath"
    fi
}

echo "Deploying state machines (stage=$STAGE, region=$AWS_REGION)"

if [[ -n "$TARGET_FILE" ]]; then
    filepath="$RENDERED_DIR/$TARGET_FILE"
    if [[ ! -f "$filepath" ]]; then
        echo "Error: $TARGET_FILE not found in $RENDERED_DIR" >&2
        exit 1
    fi
    deploy_file "$filepath"
else
    for filepath in "$RENDERED_DIR"/*.asl.json; do
        [[ -f "$filepath" ]] || continue
        deploy_file "$filepath"
    done
fi

echo "Done."
