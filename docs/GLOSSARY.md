# Glossary

The vocabulary this repo is written in. Use these terms in code, comments, commit
messages, and issues; the _Avoid_ lines record names that were considered and
rejected, usually because they are ambiguous or already mean something else here.

## Domain

**Source**:
A satellite mission or product provider whose granules are ingested as input (e.g. S6, S6B, GSFC, NASA-SSH, S3B). Identified by name throughout configuration.
_Avoid_: mission, provider, dataset

**Granule**:
A single upstream L2P file representing one observation segment — typically one (cycle, pass). Multiple granules may contribute to one **daily file**.
_Avoid_: file, observation, swath

**Cycle / Pass**:
Orbital identifiers carried by satellite altimetry granules. A pass is a single ascending or descending track; a cycle is a repeat period covering the full ground track.

**Daily file**:
The per-(source, date) NetCDF artifact produced by the pipeline. Has three lifecycle stages: **P1** (raw, written by daily_files), **P2** (OER-corrected), **P3** (finalized).
_Avoid_: daily product, day file

**P1 / P2 / P3**:
The three lifecycle versions of a daily file, written under `daily_files/{p1,p2,p3}/...`. P1 is raw daily aggregation; P2 has crossover-driven OER correction applied; P3 is the finalized output (offsets applied, bad passes flagged).

**Crossover** (or **xover**):
The intersection point between two satellite passes; statistics over crossovers drive **OER** correction and **bad pass** flagging.

**Crossover type**:
How a **Source**'s crossovers are computed — `self` or `reference`, selected via `crossover_type` in xover config. Orthogonal to **Product type**: it names what a pass is crossed *against*, not what the source is. A `reference` **Product type** Source uses `self` crossovers; a `high_latitude` Source uses `reference` crossovers.
_Avoid_: crossover mode, xover kind

**Self-crossover**:
A **Crossover** between two passes of the *same* **Source** (opposite directions, different cycles), within one orbital-cycle time window. Used by `reference` product-type sources (S6, S6B, GSFC). `crossover_type: self`.

**Reference-mission crossover**:
A **Crossover** between a `high_latitude` Source pass and the **reference mission**. Because the reference mission repeats its ground track each cycle, the crossing is observed once before and once after the high-lat time; the reference ssha is linearly interpolated *in time* to the high-lat crossover time (never extrapolated), giving `dssh = high-lat ssha − time-interpolated reference ssha`. `crossover_type: reference`.
_Avoid_: high-latitude crossover (names the satellite, not what it crosses)

**Reference mission**:
The calibration baseline a `high_latitude` Source is compared against: the finalized, unified **NASA-SSH** along-track product at **P3**.

**OER**:
Ocean Error Reduction — a polygon-based correction fit from crossover statistics, applied to P1 to produce P2. The stage handles both **Crossover types**, dispatched from the Source's **Product type** (`_CROSSOVER_TYPE_BY_PRODUCT_TYPE` in `oer/oer.py`): a `reference` Product type takes the `self` OER path (both crossover sides stacked with opposite sign, keyed by both trackids, backward-looking 10-day window), a `high_latitude` Source takes the `reference` OER path (single `dssh = ssh1 − ssh2` per crossover keyed by the high-lat trackid, no sign-flip, centered window). Both fit the same `oerfit` cubic spline, whose knot placement uses the Source's canonical `common.ground_speed` (km/s, shared with daily-file smoothing).

**Bad pass**:
A satellite pass flagged as anomalous because its crossover statistics exceed configured thresholds. Set as `nasa_flag=1` for matching cycle/pass rows during finalization.

**NASA-SSH**:
The unified along-track product. Not a real upstream source — produced by the **unifier** stage copying P3 files from contributing sources (currently GSFC, S6) under a `NASA-SSH` prefix.

**Product type**:
The product family a **Source** contributes to. Currently `reference` (TOPEX/Poseidon-baselined missions: S6, S6B, GSFC) or `high_latitude` (AVISO L2P sources: S3B, planned S3A/SARAL/HY-2B). Determines the daily-file filename family (`alt_ref_at_*` vs `alt_hilat_at_*`), the processor class, and which downstream stages apply.
_Avoid_: product family, mission class

**L2P**:
The AVISO Level-2P along-track wire format. Each L2P granule is one (cycle, pass) NetCDF carrying SSHA, mean sea surface, inter-mission bias, and a single-column validation flag. Used by all current and planned `high_latitude` sources.

**MSS** (Mean Sea Surface):
The static reference surface SSHA is measured against. Different upstream products use different MSS references; pipeline output is normalized to **DTU21** so cross-source SSHA comparison is meaningful.

**DTU21**:
The project-wide canonical MSS reference. Reference-mission processors swap to it via precomputed `<source_mss>_minus_DTU21.nc` diff files; AVISO L2P processors interpolate it directly from a bundled global grid at the L2P granule's lon/lat (see [ADR 0002](adr/0002-aviso-l2p-mss-handling.md)).
_Avoid_: target MSS (when DTU21 is meant)

## Pipeline planning

**Discovery**:
The act of finding which upstream granules exist for a source over a date range. Implementations vary by protocol (CMR, THREDDS, S3 listing) — selected via `discovery_type` in source config.

**Enumerator**:
The adapter at the discovery seam. Given a source and a date range, returns the granules that exist upstream. One concrete enumerator per upstream protocol (`CMREnumerator`, `ThreddsEnumerator`, `S3BucketEnumerator`).

**Granule reference**:
Pipeline_init's internal record of a discovered granule: `(date, uri, mod_time, ...)`. Only the URI is serialized into the **manifest**; mod_time is consumed during planning, not propagated.

**Manifest** (or **jobs manifest**):
The JSON file written to S3 by pipeline_init at `pipeline_runs/{source}/{run_id}/jobs.json`, consumed as the Distributed Map item source by the daily_files state machine. The seam between planning (pipeline_init) and execution (daily_files).

**Job spec**:
One entry in the manifest. Shape: `{date, source, bucket, granules: [uri, ...]}`. Carries exactly what daily_files needs to fetch and process a single (source, date) — nothing more.

## Results & reconciliation

**Product pipeline**:
A processing track within the top-level **pipeline** that owns one **manifest** and produces a coherent family of deliverables, reconciled independently. Two today: the *along-track product pipeline* (`at_pipeline` → P3 daily files, plus the NASA-SSH unified P3 via the unifier) and the *gridded product pipeline* (`sg_pipeline` → simple grids, ENSO, indicators). The unifier is currently wired at the top tier rather than inside `at_pipeline` (see [ADR 0003](adr/0003-failure-surfacing.md)); the along-track Run summary claims its outcomes via a stage→product-pipeline ownership mapping regardless.
_Avoid_: sub-pipeline, parent SM, stage group.

**Job outcome**:
The produced-side counterpart of a **Job spec** — what a deliverable **stage** declares after processing one item. Shape: `{schema_version, stage, status, date, source, outputs: [...], metadata: {...}}`. Returned by the handler and persisted per item in the Distributed Map's `ResultWriter` `SUCCEEDED_n.json`; the success-path analog of the structured `FAILED_n.json` entry ([ADR 0003](adr/0003-failure-surfacing.md)).
_Avoid_: result (reserved for ResultWriter), job result, production record.

**Output**:
One produced artifact named inside a **Job outcome**: `{key, kind, ...}`. The producing **stage** declares the S3 key it wrote (it already computed it via `pipeline_layout`), so consumers never reconstruct or infer filenames.
_Avoid_: file, artifact, product (when the S3 object is meant).

**Run summary**:
The per-**Product pipeline** reconciliation of expected vs produced — **Job specs** from the **manifest** against **Job outcomes** from `ResultWriter` — grouped by **stage**/deliverable, with skipped or missing items and their reasons. What **failure_handling** renders into the success SNS notification, replacing the prior S3-listing-and-`YYYYMMDD`-token-matching inference.
_Avoid_: report, status (overloaded), manifest.

**processing_history**:
A daily-file NetCDF global attribute the pipeline owns, distinct from the externally-constrained `history` field. Each file-writing **stage** (`daily_files`→P1, `oer`→P2, `finalizer`→P3) *appends* a machine-readable step rather than overwriting, so the full lineage survives to P3 and — via the unifier's byte-copy — into the NASA-SSH product. The provenance carrier a deliverable stage reads back into a **Job outcome**'s `metadata`. Absent on any product written before this attribute existed (or reprocessed from a pre-feature version); consumers treat absence as *unknown/legacy* lineage — never an error — and distinguish it from complete lineage via the per-step `product_generation_step` (gap detection → `provenance_complete`).
_Avoid_: history (the CF/external field), provenance log.

## Runtime

**pipeline_runtime**:
The shared Docker image (and parallel `pip install` layer for stage venvs) that pins the scientific-stack versions used by every stage that reads or writes a **daily file** — currently `numpy`, `xarray`, `netCDF4`, `h5py`, `h5netcdf`, `s3fs`, `pyyaml`, `boto3`. Load-bearing: the daily-file NetCDF encoding requires the writer/reader libraries to agree across stages, so this is the single contract that enforces it. Stage Dockerfiles `FROM` this image and add only their per-stage extras (e.g. `geopandas`, `cartopy`). The lightweight stages (`pipeline_init`, `unifier`) deliberately do **not** consume pipeline_runtime — they touch no NetCDF and stay on `lambda/python` directly to keep their cold-start small. See [ADR 0001](adr/0001-pipeline-runtime.md).

## Build & deploy

**Target**:
Anything the build/deploy scripts manage — a buildable image and/or a deployable Lambda — identified by name = the basename of its source directory (e.g. `oer`, `daily_files`, `pipeline_runtime`, `failure_handling`). Every target has a **packaging kind** and a `deployable` fact. Most targets are pipeline **stages**; `pipeline_runtime` is the sole non-deployable base image; the `pipeline/infra/` Lambdas are the zip-packaged targets.
_Avoid_: artifact, service, job

**Packaging kind**:
How a **Target** is built and shipped: `container` (Dockerfile → buildx → ECR → `update-function-code --image-uri`) or `zip` (source dir → zip → `update-function-code --zip-file`, no ECR). Derived, not declared: a Dockerfile ⇒ `container`; an `app.py` under `infra/` with no Dockerfile ⇒ `zip`. The packaging seam is where the build and deploy adapters differ.

**Image**:
A **Target** with `container` packaging — a buildable Docker artifact. Carries the `heavy` build fact. Drives the build phase (zip targets have no image). `pipeline_runtime` is an Image that is not a **Deploy target**.
_Avoid_: container

**Heavy** (image/stage):
An Image whose Dockerfile parameterizes its base via `ARG BASE_IMAGE` / `FROM ${BASE_IMAGE}`, so the build can override it to the matching `pipeline_runtime` tag. The 8 daily-file + downstream-product stages are heavy; `pipeline_init` and `unifier` are lightweight (literal `FROM lambda/python`); `pipeline_runtime` is the base itself; `zip` targets are never heavy. A heavy Image depends on `pipeline_runtime` at build time.
_Avoid_: scientific stage, big image

**Deploy target**:
A **Target** that maps to a `${stage}-${name}` Lambda the deploy step updates — i.e. one with `deployable: true`. Every Image except `pipeline_runtime`, plus every `zip` infra Lambda. `deployable` is the one genuinely-external fact (it reflects what exists in AWS), so it is declared, not inferred; `pipeline_runtime` is the only `deployable: false` entry.

**Target registry**:
The single module owning the catalog of **Targets**. Existence and **packaging kind** are derived from the filesystem (Dockerfile vs infra `app.py`); the `heavy` / `deployable` facts are declared in `targets.yaml` (path-less, so it survives directory moves); a consistency test pins the manifest to the filesystem. Exposes the catalog (with `ecr_repo` / `function_name` naming helpers) and a **change-impact** query, replacing the hand-maintained `HEAVY_STAGES` / `BUILD_ONLY_IMAGES` arrays, the scattered `find … -name` directory lookups, and the duplicated `${stage}/${name}` naming conventions. See [ADR 0004](adr/0004-target-registry.md).
_Avoid_: image list, manifest (reserved for the jobs manifest)

**Change-impact**:
The Target registry's pure function from a set of changed paths to the set of dirty **Targets** that must be rebuilt/redeployed. Encodes the per-packaging dependency edges: a `container` stage is dirty if its own dir changed, or shared `utilities/` + root `pyproject.toml` changed, or (if **heavy**) `pipeline_runtime/` changed; a `zip` target is dirty only if its own dir changed (self-contained). If any **heavy** target is in the result, `pipeline_runtime` is also included — the SHA-tagged ECR image must exist even when the runtime's own content is unchanged. Replaces the dev loop's leaky `git diff -- "$DIR"` gate.

## Failure handling

**Code failure**:
A failure where the Lambda handler's `try/except` ran and packaged the error — `errorType`, `errorMessage`, and the failing `input` are surfaced as JSON in the raised exception. Appears in the Distributed Map's `ResultWriter` output (`FAILED_n.json`) with that structured shape. Action: developer fix.
_Avoid_: bug, exception, handler error

**Runtime failure**:
A failure where the Lambda runtime killed the process before the handler's `try/except` could run — typically `Lambda.Timeout`, `Lambda.OOM`, or init errors. The `FAILED_n.json` carries only the raw Step Functions `Cause`; no `errorType` / `input` from the handler shape. Action: config fix (timeout, memory, warmup, Provisioned Concurrency).
_Avoid_: lambda error, timeout (when meant generically)

**Auth failure**:
A subtype of **code failure** where the underlying cause is upstream credentials — typically a `ClientError` / `HTTPError` with 401/403 from PODAAC, or `podaac_auth` itself raising. Distinguished from generic code failures because the action is to rotate creds or check that `podaac_auth` ran, not to fix code.
_Avoid_: credentials error

**failure_handling**:
The Lambda invoked from a parent state machine's `Catch` that classifies failed items as **Code failure** / **Runtime failure** / **Auth failure**, deduplicates them by `(category, errorType, errorMessage)`, and publishes one SNS notification per failed pipeline run. Reads the Distributed Map's `ResultWriter` output from S3 to surface per-item detail; degrades to the top-level `Cause` when no per-item output exists (`pipeline_init` failures, `ItemReader` failures, runtime failures without item context). See [ADR 0003](adr/0003-failure-surfacing.md).
_Avoid_: error_handler, on_failure

## Relationships

- A **Source** has a **Product type**; the **Product type** determines the **Daily file**'s filename family and which processor class handles it.
- A **Source** has many **Granules** upstream; an **Enumerator** finds them.
- A **Granule** contributes to exactly one **daily file** (date-keyed).
- A **Daily file** progresses **P1** → **P2** → **P3**; may then be copied to **NASA-SSH** by the unifier.
- The **Manifest** carries one **Job spec** per (source, date) needing processing; each **Job spec** lists the **Granule** URIs that compose that day's daily file.
- **Crossover** statistics drive both **OER** (which produces P2) and **bad pass** flagging (consumed during finalization to produce P3).
- A **Source**'s **Crossover type** follows from its **Product type**: a `reference` Source uses **Self-crossovers**; a `high_latitude` Source uses **Reference-mission crossovers** against the **reference mission** (NASA-SSH P3).
- A pipeline-run failure is classified as a **Code failure**, **Runtime failure**, or **Auth failure**; **failure_handling** groups failed items by this classification in the SNS notification.
- The top-level **pipeline** is composed of **Product pipelines**; each owns one **manifest** and yields a deliverable family.
- A deliverable **stage** declares one **Job outcome** per processed item (naming its **Outputs**), persisted in `ResultWriter`; a **Product pipeline**'s **Run summary** reconciles those **Job outcomes** against its **manifest**'s **Job specs**.
- Provenance rides in the daily file's **processing_history** attribute P1→P2→P3 and survives the unifier's byte-copy into NASA-SSH; a deliverable **stage** snapshots it into the **Job outcome**'s `metadata`.

## See also

- [DATA_FLOW.md](DATA_FLOW.md) — how these pieces connect, as a diagram
- [adr/](adr/) — the decisions behind them
