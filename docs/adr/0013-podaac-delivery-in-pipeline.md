# ADR 0013: PO.DAAC delivery moves into the pipeline, audited against CMR

- **Status**: Accepted. The CMR audit is implemented; the delivery stage and key rotation are not.
- **Date**: 2026-11-20

## Context

Released products reach PO.DAAC through a weekly cron on climateelvis (`delivery-scripts`):
it syncs the whole prod bucket, stages files modified since the last delivery, and uploads
them with PO.DAAC-issued access keys held in a local AWS profile, which a second daily cron
rotates. Its recent fixes (a recorded `delivered_through`, a settle window) stopped the silent
losses its earlier timestamp cutoff caused, but delivery still runs on its own clock. A pipeline
run that finishes after the Monday 07:30 cron, or a hand-triggered run or reprocess, waits a
week: 10-26 was published at 15:37 and ingested 162 hours later. Every cadence change has to be
mirrored in the cron. Delivery failures show only in a log on climateelvis, and what is new is
still inferred from modification times rather than from what the publisher released.

The cheap alternative was to run the climateelvis cron daily: a worst-case lag of a day and no
new infrastructure. It was rejected because it keeps delivery invisible to the run, coupled to
the pipeline only by timing, and in need of retiming whenever the cadence changes.

## Decision

**Delivery becomes a pipeline step after publication**, driven by the publisher's own results
rather than by modification times, so it delivers when products are produced, gets Step
Functions retries, and reports in the run summary.

**Only prod can deliver.** The PO.DAAC keys live in a Secrets Manager secret that exists only
in the prod account and is readable only by the prod delivery and rotation Lambdas, and the
PO.DAAC bucket is set only in the prod deployment. A dev deployment of the same stage has no
credentials and no target, so no flag or input can make it deliver.

**The existing PO.DAAC keys, not a cross-account role.** A role from PO.DAAC would remove
static keys entirely but depends on PO.DAAC's timeline. The keys keep rotating themselves as
`rotate_access_keys.py` does on climateelvis, ported to a Lambda that calls IAM with a session
built from the secret's own value. Secrets Manager's managed rotation was rejected: its schedule
cannot delete the deactivated key at 75 days, and driving its four-step protocol from a daily
schedule adds a state machine for nothing.

**Delivery is opt-in per published line**, declared under `delivery:` in
`published_products:`, one entry per PO.DAAC collection. It is not inferred from `id` being
settled: a DOI can be settled before PO.DAAC creates the collection, and
`along_track_high_latitude` is one Product shared by every high-latitude satellite while each
satellite gets its own collection. A line with no declaration is published and never
delivered.

**An audit against CMR, run on a schedule rather than in the pipeline.** Delivery succeeding
does not mean PO.DAAC ingested the file, and a run that fails after publication delivers
nothing, so the audit compares `products/` against CMR whatever happened to the run. CMR is the
measure of success, because a file in PO.DAAC's bucket proves upload, not ingest. Once the
delivery Lambda exists, the audit looks in PO.DAAC's bucket for each file CMR lacks, through
that Lambda's read-only check mode, to say whether a gap is ours (never delivered, or delivered
out of date) or PO.DAAC's (delivered, not ingested). It does not hold the PO.DAAC key itself,
since that key can also write. An audit at the end of a run was rejected because ingest lags
delivery by hours and the run would see its own files as pending. One on climateelvis was
rejected because it would fail silently with the delivery it checks.

**One audit invocation over every delivered collection, not a Step Functions Map.** The
collections are independent for the audit's purpose even though the lines' production is not:
each compares one prefix's released files with one collection's granules. Even with every
high-latitude line delivered the audit fits a Lambda's time and memory, so a Map would add a
state machine and an aggregator to produce one email. Isolation comes from catching each
collection's failure and reporting it beside the others' results. `audit_collection` is the
unit of work, so it can become a Map's item body if the audit ever outgrows one Lambda.

## Consequences

- The audit emails on every outcome, including when it cannot run, so a missing email means
  the schedule or the Lambda stopped. It refuses a CMR listing shorter than `CMR-Hits` rather
  than reporting the truncated remainder as missing.
- Onboarding a line at PO.DAAC is a `delivery:` entry in `products.yaml`; the audit picks it up
  with no schedule edit.
- climateelvis keeps the full-bucket sync permanently, since validation users read the staging
  prefixes from it. Only its delivery and rotation retire.
- The plan and its phases are in `.claude/PLAN_podaac_delivery_in_pipeline.md`.
