#!/usr/bin/env bash
set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Load environment ────────────────────────────────────────────────
source "$REPO_ROOT/scripts/util/load_env.sh"

# ── Defaults ────────────────────────────────────────────────────────
STAGE="dev"
TARGET_FILE=""

# ── Parse arguments ─────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --stage)  STAGE="$2";       shift 2 ;;
        --file)   TARGET_FILE="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--stage dev|prod] [--file <name>.asl.json]"
            echo ""
            echo "Renders state machine templates into state_machines/rendered/"
            echo "with environment-specific values substituted."
            echo ""
            echo "Options:"
            echo "  --stage   Deployment stage (default: dev)"
            echo "  --file    Render a single template instead of all"
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

# ── Render ──────────────────────────────────────────────────────────
OUT_DIR="$SCRIPT_DIR/rendered"
mkdir -p "$OUT_DIR"

# Substitute only these three variables; leave $LATEST, $states, etc. alone.
# Uses sed instead of envsubst for portability (no gettext dependency).
render_file() {
    local src="$1"
    local name
    name="$(basename "$src")"
    sed \
        -e "s/\${AWS_ACCOUNT_ID}/$AWS_ACCOUNT_ID/g" \
        -e "s/\${AWS_REGION}/$AWS_REGION/g" \
        -e "s/\${STAGE}/$STAGE/g" \
        "$src" > "$OUT_DIR/$name"
    echo "  rendered → rendered/$name"
}

echo "Rendering state machines (stage=$STAGE, region=$AWS_REGION)"

if [[ -n "$TARGET_FILE" ]]; then
    src="$SCRIPT_DIR/$TARGET_FILE"
    if [[ ! -f "$src" ]]; then
        echo "Error: $TARGET_FILE not found in $SCRIPT_DIR" >&2
        exit 1
    fi
    render_file "$src"
else
    for src in "$SCRIPT_DIR"/*.asl.json; do
        [[ -f "$src" ]] || continue
        render_file "$src"
    done
fi

echo "Done."
