# ADR 0003: Pipeline failure surfacing via parent-SM Catch into a shared `failure_handling` Lambda

- **Status**: Accepted
- **Date**: 2026-05-28

## Context

When a stage Lambda errors inside a Distributed Map, the failure propagates up through nested state machines (`pipeline.asl.json` → `at_pipeline.asl.json` → `daily_file.asl.json` → Map → item) before the top-level execution is marked failed. Each layer's `Catch` currently routes directly to a `Fail` state with no enrichment. Diagnosing root cause requires 5+ console hops: top-level execution → child SM link → child execution → Map → failed item → Lambda invocation → CloudWatch Logs.

Operators care about two outcomes from a failed run:

1. **Notification** that processing stopped (latency-sensitive; control-plane).
2. **Root cause** — what failed, what the input was, why (data-plane lookup).

A `failure_handling` Lambda has existed in `pipeline/infra/failure_handling/` since the early pipeline build but is wired into no state machine. The `Catch` blocks in `pipeline.asl.json`, `at_pipeline.asl.json`, and `sg_pipeline.asl.json` all point straight at `Fail`.

Three failure modes drive the design (see CONTEXT.md: **Code failure**, **Runtime failure**, **Auth failure**):

- **Code failure** — handler's `try/except` ran; the raised exception carries our structured `{errorType, errorMessage, input}` JSON. Per-item detail is in the Distributed Map's `ResultWriter` output as `FAILED_*.json`.
- **Runtime failure** — Lambda runtime killed the process (timeout, OOM, init error) before `try/except` could run. The `FAILED_*.json` carries only the raw Step Functions `Cause`.
- **Auth failure** — a subtype of code failure where the underlying cause is upstream creds (PODAAC 401/403, or `podaac_auth` itself raising).

A key architectural fact constrained the design: **Distributed Map drops per-item context at the parent SM boundary.** The parent SM's Catch sees a generic `States.ExceedToleratedFailureThreshold` Cause naming the child execution ARN; the per-item failures live only in the Map's S3 ResultWriter output. No amount of Catch wiring at the parent or top tier recovers that detail through Step Functions alone — a data-plane S3 read is required.

## Decision

### Topology

Wire `failure_handling` into **parent-tier `Catch` blocks**:

- `at_pipeline.asl.json` (7 stage Tasks: Init pipeline, Daily File Execution, Xover 1, OER, Xover 2, Bad Pass, Finalizer).
- `sg_pipeline.asl.json` (4 stage Tasks: Set SG Jobs, Simple Grids Execution, ENSO Execution, Indicators).
- `pipeline.asl.json` (Unifier Execution only). The Unifier has no intermediate parent SM — pipeline.asl.json directly calls the leaf `unifier-sm` — so pipeline.asl.json *is* the parent tier for unifier failures. The other two top-tier Catches (Along Track Execution, SG Execution) keep pointing at `Fail`: their child SMs (`at_pipeline`, `sg_pipeline`) already notify from their own parent-tier Catches; a top-tier notification would duplicate.

Total: 12 Catches wired across three ASL files. Leaf SMs (`daily_file`, `xover`, `oer`, `bad_pass`, `finalizer`, `unifier`, `simple_grids`, `enso`) cannot host a Catch at the Map level for per-item context, so they are not modified.

### Per-SM wiring pattern

One shared `Notify Failure` state per parent SM. Each stage Task's `Catch` sets `Output` to carry the stage identifier plus the run inputs and `$states.errorOutput`:

```jsonc
"Daily File Execution": {
  "Type": "Task",
  "Resource": "arn:aws:states:::states:startExecution.sync:2",
  // ...existing Arguments...
  "Catch": [{
    "ErrorEquals": ["States.ALL"],
    "Next": "Notify Failure",
    "Output": "{% { 'stage': 'daily_file', 'errorOutput': $states.errorOutput, 'jobs_key': $states.input.jobs_key, 'bucket': $states.input.bucket, 'source': $states.input.source } %}"
  }]
},
"Notify Failure": {
  "Type": "Task",
  "Resource": "arn:aws:states:::lambda:invoke",
  "Arguments": {
    "FunctionName": "arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:function:${STAGE}-failure_handling:$LATEST",
    "Payload": "{% $states.input %}"
  },
  "Next": "Fail"
},
"Fail": { "Type": "Fail" }
```

### `failure_handling` behavior

1. Read input `{stage, errorOutput, jobs_key, bucket, source}`.
2. Derive `run_id` from `jobs_key` (segment 2 of `pipeline_runs/{source}/{run_id}/jobs.json`).
3. Compute the ResultWriter prefix inline: `pipeline_runs/{source}/{run_id}/results/{stage}/`. The shape mirrors `utilities/pipeline_layout.py:stage_results_prefix(source, run_id, stage)` but is duplicated here because `failure_handling` is an infra Lambda that does **not** ship the `utilities` package. A code comment in `failure_handling/app.py` points at the helper as the canonical reference.
4. `s3:ListObjectsV2` under that prefix; collect any keys containing `/FAILED_`. The MapRunArn subdirectory is traversed implicitly — no Step Functions API call.
5. Read each `FAILED_*.json`. Each entry's `Cause` is a JSON-stringified payload; parse it to extract `errorType`, `errorMessage`, `input`.
6. Classify each failed item:
   - `errorType == "PipelineError"` → **Code failure**.
   - `errorType` starts with `Lambda.` (`Lambda.Timeout`, `Lambda.OOM`, …) → **Runtime failure**.
   - **Code failure** *and* `errorMessage` matches `r"(401|403|Unauthorized|Forbidden)"` → **Auth failure**.
7. Deduplicate by `(category, errorType, errorMessage)`. Each distinct failure becomes one entry with a count, a sample input, and the list of affected dates.
8. If `ListObjectsV2` returns no `FAILED_*.json` (e.g. `pipeline_init` raised before any Map ran, or `ItemReader` failed), fall back to a single entry derived from `errorOutput.Cause` and `errorOutput.Error`.
9. Compose an SNS message:
   - Header: `stage`, `source`, `run_id`, top-level `Cause`, child execution ARN (parsed from Cause when present), CloudWatch deep-link.
   - Body: up to 10 distinct failures in the message; if more, "and N more — see {s3 URL of full manifest}" overflow.
10. `sns:Publish` the message. Wrap the publish in `try/except`; log to CloudWatch on failure. **Always return success** — the parent SM owns the `Fail` transition, and a failure_handling exception would mask the original failure.

### Exception class change

Introduce one shared `PipelineError(Exception)` class in `utilities/errors.py`. Every containerized stage handler (`daily_files`, `xover`, `oer`, `bad_pass`, `finalizer`, `unifier`, `simple_grids`, `enso`, `indicators`) raises `PipelineError(json.dumps(error_response))` instead of `Exception(json.dumps(error_response))`. `unifier` previously had no `try/except` wrap; this ADR adds one for consistency so the **input** field (date, source) propagates through to the SNS message's affected-dates summary.

The class name itself is the machine-readable signal `failure_handling` uses to distinguish **Code failures** from **Runtime failures** (`Lambda.Timeout`, `Lambda.OOM`). No per-stage subclasses — `failure_handling` already parses `errorType` from the packaged payload.

### IAM

`failure_handling` requires `sns:Publish` (already configured), `s3:ListBucket` and `s3:GetObject` on the pipeline bucket. No Step Functions API permissions are needed.

## Consequences

**Positive:**
- One SNS notification per failed pipeline run, carrying classified and deduplicated per-item detail.
- Operator workflow collapses from 5+ console hops to one notification + at most one S3 link for overflow.
- Adding a new stage requires one `Catch` block on the new Task; the `Notify Failure` state is reused.
- `failure_handling`'s only AWS dependencies are SNS and S3 — no Step Functions API, so no IAM expansion beyond the existing bucket policies.
- The failure taxonomy in CONTEXT.md is now codified in code (`PipelineError`), in the SNS message format, and in any future runbook.

**Negative:**
- `failure_handling`'s input shape becomes a load-bearing interface across 12 Catch wirings. Changing it later means touching every Catch.
- The ResultWriter prefix layout is now duplicated in `pipeline_layout.py:stage_results_prefix` and in each leaf ASL's JSONata. A future cleanup could have `pipeline_init` compute and pass the prefix as an input parameter, eliminating the JSONata version. Out of scope here.
- `pipeline_layout.py:stage_results_prefix` currently has the wrong signature (`stage_results_prefix(stage)` returning a source/run_id-less path). This ADR's implementation updates it to `stage_results_prefix(source, run_id, stage)` and updates `utilities/tests/test_pipeline_layout.py` to match. Any caller relying on the old signature must be updated; a grep shows only the test references it today.
- `failure_handling` invocation adds ~one Lambda invocation of latency between failure and `Fail` transition. Acceptable: failures are exceptional and the parent SM already has the failed state.

## Alternatives considered

- **Wire `failure_handling` into `pipeline.asl.json`'s top-tier Catches instead.** Rejected: the top tier's Cause only names the child SM (e.g. `at_pipeline`), not the failing stage (e.g. `daily_file`). The parent tier knows the stage by virtue of the Catch's State Name, which is essential for computing the ResultWriter prefix.
- **Wire `failure_handling` at both top and parent tiers.** Rejected: duplicate SNS notifications for the same failure.
- **Per-stage `Notify Failure` states (`Notify Daily File Failure`, `Notify Xover Failure`, …).** Rejected: adds ~11 new states across two ASLs. One shared state per parent SM with `stage` passed as a Catch `Output` literal achieves the same and keeps the ASL smaller.
- **Per-Lambda exception subclasses (`DailyFileError`, `XoverError`, …) as the original ticket proposed.** Rejected: `failure_handling` already parses `errorType` from each handler's structured JSON payload (`type(e).__name__`), so the subclass adds no signal the SNS path can use. The only place subclasses would be visible is the SM event history view in the console — which the new SNS workflow replaces. A single `PipelineError` base class is enough to distinguish **Code failure** from **Runtime failure**.
- **`LoggingConfiguration` with `level=ERROR` on every SM + a CloudWatch Logs Insights query.** Deferred to a follow-up ticket. Useful for historical analysis (failure-rate trends, which stage fails most) but does not address the immediate "what just failed and why" workflow that SNS now covers. Costs a CloudWatch log group per SM.
- **X-Ray tracing on Lambdas and SMs.** Deferred indefinitely. Helps for latency investigation, not for failure root cause now that SNS carries per-item detail.
- **`failure_handling` calls `stepfunctions:ListMapRuns` to discover the `MapRunArn` subdirectory.** Rejected: requires expanding the Lambda's IAM role with Step Functions API permissions, which is a costly change in this org. `s3:ListObjectsV2` under the ResultWriter prefix discovers the `MapRunArn` subdirectory implicitly with no new permissions.
- **Cold-start timeout reduction (raised Lambda Timeout, Provisioned Concurrency, scheduled warmup ping).** Out of scope. Cold-start timeouts are a Lambda configuration problem, not a log-surfacing problem, and surface mostly in dev (prod runs weekly so Lambdas stay warm-ish). Tracked as a separate follow-up.
