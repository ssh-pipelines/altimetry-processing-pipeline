#!/usr/bin/env bash
set -eo pipefail

# Non-mutating end-to-end smoke test.
#
# Starts a real pipeline execution over an already-processed window WITHOUT
# force_update, so every contributor's pipeline_init finds no new jobs and each
# Map iteration fast-exits down the No Jobs branch. That exercises the whole
# product-keyed orchestration — start_run's contributor resolution, the source
# tier Map, merge_manifests, run_summary, the success notification — against
# real stage infra while writing zero product data. It then asserts the run
# summary reconciles clean (every product pipeline expected==produced,
# missing==[]).
#
# Run it right after a deploy (in dev before a release, in prod right after) so
# the weekly scheduled run is a confirmation, not your first signal.
#
# Usage:
#   scripts/smoke.sh --product <NAME> --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
#                    [--stage dev|prod] [--bucket <b>] [--timeout <sec>] [--no-precheck]
#
# --product names a Published product (e.g. NASA-SSH)
#
# Pick a --start/--end window you KNOW is already processed (or has no data), so
# the run is a guaranteed no-op. By default a pre-check invokes pipeline_init
# once per contributor and aborts if any has new jobs (which would process real
# data); the pre-check writes throwaway jobs manifests under a fresh run_id
# (metadata only).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$REPO_ROOT/scripts/util/load_env.sh"

STAGE="dev"
PRODUCT=""
START=""
END=""
BUCKET="${BUCKET:-${BUCKET_NAME:-}}"
# Two tiers of nested sync executions now sit under the parent, so a fast-exit
# run has more state transitions to get through than the single-tier version.
TIMEOUT=600
PRECHECK=true

# Same convention as util/registry.sh: the interpreter that can import `utilities`.
: "${SMOKE_PY:=${PYTHON:-python3}}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --product) PRODUCT="$2"; shift 2 ;;
        --start)   START="$2";   shift 2 ;;
        --end)     END="$2";     shift 2 ;;
        --stage)   STAGE="$2";   shift 2 ;;
        --bucket)  BUCKET="$2";  shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --no-precheck) PRECHECK=false; shift ;;
        -h|--help)
            echo "Usage: $0 --product <NAME> --start <YYYY-MM-DD> --end <YYYY-MM-DD>"
            echo "          [--stage dev|prod] [--bucket <b>] [--timeout <sec>] [--no-precheck]"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

: "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
: "${AWS_REGION:?AWS_REGION not set}"
[ -n "$PRODUCT" ] || { echo "Error: --product is required" >&2; exit 1; }
[ -n "$START" ] && [ -n "$END" ] || { echo "Error: --start and --end are required" >&2; exit 1; }
[ -n "$BUCKET" ] || { echo "Error: --bucket (or BUCKET_NAME) is required to read the summary" >&2; exit 1; }

SM_ARN="arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:nasa-ssh-pipeline-${STAGE}-sm"
INPUT="$(printf '{"published_product":"%s","start":"%s","end":"%s","bucket":"%s"}' \
    "$PRODUCT" "$START" "$END" "$BUCKET")"

echo "Smoke: $PRODUCT $START..$END on stage=$STAGE (bucket=$BUCKET)"

# ── Pre-check: refuse to run if any contributor has real work (would mutate) ──
if [ "$PRECHECK" = true ]; then
    # Ask the registry the same question start_run will, rather than guessing
    # which sources feed this line.
    contributors="$(cd "$REPO_ROOT" && "$SMOKE_PY" -c '
import sys
from utilities.products import contributing_sources
print(" ".join(contributing_sources(sys.argv[1])))' "$PRODUCT")" || {
        echo "  ✗ could not resolve contributors for $PRODUCT" >&2
        echo "    (needs the utilities package importable; set SMOKE_PY=./.venv/bin/python)" >&2
        exit 1
    }
    [ -n "$contributors" ] || { echo "  ✗ no enabled contributor for $PRODUCT" >&2; exit 1; }
    echo "  contributors: $contributors"

    precheck_run_id="smoke-$(date -u +%Y%m%dT%H%M%SZ)"
    for src in $contributors; do
        echo "  pre-check: planning $src via ${STAGE}-pipeline_init ..."
        pc="$(mktemp)"
        payload="$(printf '{"source":"%s","run_id":"%s","start":"%s","end":"%s","bucket":"%s"}' \
            "$src" "$precheck_run_id" "$START" "$END" "$BUCKET")"
        ferr="$(aws lambda invoke --function-name "${STAGE}-pipeline_init" \
            --cli-binary-format raw-in-base64-out --payload "$payload" \
            --query 'FunctionError' --output text "$pc")"
        if [ "$ferr" != "None" ]; then
            echo "  ✗ pipeline_init errored during pre-check for $src:" >&2
            cat "$pc" >&2; rm -f "$pc"; exit 1
        fi
        job_count="$("$SMOKE_PY" -c 'import json,sys; print(json.load(open(sys.argv[1])).get("job_count","?"))' "$pc")"
        rm -f "$pc"
        if [ "$job_count" != "0" ]; then
            echo "  ✗ $src has $job_count new job(s); running it would process real data." >&2
            echo "    Pick an already-processed window, or pass --no-precheck to override." >&2
            exit 1
        fi
    done
    echo "  pre-check ok: 0 new jobs for every contributor — run will fast-exit."
fi

# ── Start the execution and wait ────────────────────────────────────────
exec_name="smoke-${STAGE}-$(date -u +%Y%m%dT%H%M%SZ)"
exec_arn="$(aws stepfunctions start-execution \
    --state-machine-arn "$SM_ARN" --name "$exec_name" --input "$INPUT" \
    --query executionArn --output text)"
echo "  started $exec_name"

deadline=$(( $(date +%s) + TIMEOUT ))
while :; do
    status="$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
        --query status --output text)"
    [ "$status" != "RUNNING" ] && break
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "  ✗ timed out after ${TIMEOUT}s (still RUNNING)" >&2; exit 1
    fi
    sleep 5
done

if [ "$status" != "SUCCEEDED" ]; then
    echo "  ✗ execution $status" >&2
    aws stepfunctions describe-execution --execution-arn "$exec_arn" \
        --query '{error:error,cause:cause}' --output json >&2 || true
    exit 1
fi
echo "  execution SUCCEEDED"

# ── Locate and assert the run summary ───────────────────────────────────
output="$(aws stepfunctions describe-execution --execution-arn "$exec_arn" \
    --query output --output text)"
# The terminal state is a lambda:invoke task, so the execution output is the raw
# invoke envelope — run_summary's real return (published_product/run_id) is nested
# under .Payload. Fall back to the top level in case the ASL is ever changed to
# return the payload directly.
read_field() {
    printf '%s' "$output" | "$SMOKE_PY" -c '
import json, sys
d = json.load(sys.stdin)
if isinstance(d, dict):
    d = d.get("Payload", d)
print(d.get(sys.argv[1], "") if isinstance(d, dict) else "")' "$1"
}
product="$(read_field published_product)"
run_id="$(read_field run_id)"
if [ -z "$product" ] || [ -z "$run_id" ]; then
    # Also the shape when run_summary itself failed: its Catch goes to Succeed, so
    # the execution reads SUCCEEDED with no summarized identity in the output.
    echo "  ✗ execution output had no published_product/run_id: $output" >&2; exit 1
fi

key="pipeline_runs/${product}/${run_id}/summary.json"
summary="$(mktemp)"
if ! aws s3api get-object --bucket "$BUCKET" --key "$key" "$summary" >/dev/null 2>&1; then
    echo "  ✗ succeeded but no summary at s3://$BUCKET/$key" >&2; rm -f "$summary"; exit 1
fi

"$SMOKE_PY" - "$summary" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))
pps = s.get("product_pipelines", {})
bad = []
for name, sec in pps.items():
    exp, prod, miss = sec.get("expected", 0), sec.get("produced", 0), sec.get("missing", [])
    if miss or exp != prod:
        bad.append((name, exp, prod, len(miss)))
if bad:
    print("  ✗ run summary reconciliation failed:")
    for n, e, p, m in bad:
        print(f"      {n}: expected={e} produced={p} missing={m}")
    sys.exit(1)
contributors = ", ".join(s.get("contributors", [])) or "none"
print(f"  ✓ run summary clean for {s.get('published_product')} {s.get('run_id')} "
      f"(contributors: {contributors}; {len(pps)} product pipeline(s); "
      f"all expected==produced, missing=[])")
PY
rm -f "$summary"

echo "SMOKE PASSED ($PRODUCT, stage=$STAGE)"
