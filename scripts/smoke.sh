#!/usr/bin/env bash
set -eo pipefail

# Non-mutating end-to-end smoke test.
#
# Starts a real pipeline execution over an already-processed window WITHOUT
# force_update, so pipeline_init finds no new jobs and the run fast-exits straight
# to run_summary. That exercises the whole orchestration — pipeline_init, the
# Has Jobs?/No Jobs wiring, run_summary, the success notification — against real
# stage infra while writing zero product data. It then asserts the run summary
# reconciles clean (every product pipeline expected==produced, missing==[]).
#
# Run it right after a deploy (in dev before a release, in prod right after) so
# the weekly Monday run is a confirmation, not your first signal.
#
# Usage:
#   scripts/smoke.sh --source <NAME> --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
#                    [--stage dev|prod] [--bucket <b>] [--timeout <sec>] [--no-precheck]
#
# Pick a --start/--end window you KNOW is already processed (or has no data), so
# the run is a guaranteed no-op. By default a pre-check invokes pipeline_init and
# aborts if the window has new jobs (which would process real data); the
# pre-check writes a throwaway jobs manifest under a fresh run_id (metadata only).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$REPO_ROOT/scripts/util/load_env.sh"

STAGE="dev"
SOURCE=""
START=""
END=""
BUCKET="${BUCKET:-${BUCKET_NAME:-}}"
TIMEOUT=300
PRECHECK=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)  SOURCE="$2";  shift 2 ;;
        --start)   START="$2";   shift 2 ;;
        --end)     END="$2";     shift 2 ;;
        --stage)   STAGE="$2";   shift 2 ;;
        --bucket)  BUCKET="$2";  shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --no-precheck) PRECHECK=false; shift ;;
        -h|--help)
            echo "Usage: $0 --source <NAME> --start <YYYY-MM-DD> --end <YYYY-MM-DD>"
            echo "          [--stage dev|prod] [--bucket <b>] [--timeout <sec>] [--no-precheck]"
            exit 0
            ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

: "${AWS_ACCOUNT_ID:?AWS_ACCOUNT_ID not set}"
: "${AWS_REGION:?AWS_REGION not set}"
[ -n "$SOURCE" ] || { echo "Error: --source is required" >&2; exit 1; }
[ -n "$START" ] && [ -n "$END" ] || { echo "Error: --start and --end are required" >&2; exit 1; }
[ -n "$BUCKET" ] || { echo "Error: --bucket (or BUCKET_NAME) is required to read the summary" >&2; exit 1; }

SM_ARN="arn:aws:states:${AWS_REGION}:${AWS_ACCOUNT_ID}:stateMachine:nasa-ssh-pipeline-${STAGE}-sm"
INPUT="$(printf '{"source":"%s","start":"%s","end":"%s","bucket":"%s"}' "$SOURCE" "$START" "$END" "$BUCKET")"

echo "Smoke: $SOURCE $START..$END on stage=$STAGE (bucket=$BUCKET)"

# ── Pre-check: refuse to run if the window has real work (would mutate) ──
if [ "$PRECHECK" = true ]; then
    echo "  pre-check: planning jobs via ${STAGE}-pipeline_init ..."
    pc="$(mktemp)"
    ferr="$(aws lambda invoke --function-name "${STAGE}-pipeline_init" \
        --cli-binary-format raw-in-base64-out --payload "$INPUT" \
        --query 'FunctionError' --output text "$pc")"
    if [ "$ferr" != "None" ]; then
        echo "  ✗ pipeline_init errored during pre-check:" >&2; cat "$pc" >&2; rm -f "$pc"; exit 1
    fi
    job_count="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("job_count","?"))' "$pc")"
    rm -f "$pc"
    if [ "$job_count" != "0" ]; then
        echo "  ✗ window has $job_count new job(s); running it would process real data." >&2
        echo "    Pick an already-processed window, or pass --no-precheck to override." >&2
        exit 1
    fi
    echo "  pre-check ok: 0 new jobs — run will fast-exit."
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
# invoke envelope — run_summary's real return (source/run_id) is nested under
# .Payload. Fall back to the top level in case the ASL is ever changed to return
# the payload directly.
read_field() {
    printf '%s' "$output" | python3 -c '
import json, sys
d = json.load(sys.stdin)
if isinstance(d, dict):
    d = d.get("Payload", d)
print(d.get(sys.argv[1], "") if isinstance(d, dict) else "")' "$1"
}
src="$(read_field source)"
run_id="$(read_field run_id)"
if [ -z "$src" ] || [ -z "$run_id" ]; then
    echo "  ✗ execution output had no source/run_id: $output" >&2; exit 1
fi

key="pipeline_runs/${src}/${run_id}/summary.json"
summary="$(mktemp)"
if ! aws s3api get-object --bucket "$BUCKET" --key "$key" "$summary" >/dev/null 2>&1; then
    echo "  ✗ succeeded but no summary at s3://$BUCKET/$key" >&2; rm -f "$summary"; exit 1
fi

python3 - "$summary" <<'PY'
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
print(f"  ✓ run summary clean for {s.get('source')} {s.get('run_id')} "
      f"({len(pps)} product pipeline(s); all expected==produced, missing=[])")
PY
rm -f "$summary"

echo "SMOKE PASSED ($SOURCE, stage=$STAGE)"
