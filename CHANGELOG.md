# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

High-latitude (S3B) support is still a work in progress and is not yet enabled in
production. The entries below ship in the code but remain unannounced until that
source is fully supported.

### Changed
- **Pipeline runs are triggered by published product, not by source.** The trigger
  input changes from `{source: "S6"}` to `{published_product: "NASA-SSH"}`, and
  `pipeline.asl.json` composes two independently optional tiers over it: a *source
  tier* Mapped across the line's `enabled` contributors (planning, the along-track
  chain, publication) and a *product tier* run once over the line (simple grids,
  their promotion, ENSO, indicators).

  **The 16 EventBridge schedules must be updated to the new input shape**; nothing
  in the repo deploys them. They are now mirrored in `utilities/schedules.yaml` so
  the trigger shape is reviewable, with a test pairing them against the source
  registry in both directions.

  A high-latitude satellite's line has no product tier, so ENSO and indicators are
  unreachable from a per-satellite trigger rather than fatal — which is what makes
  `enabled: true` safe to set on a high-latitude source. `product_type` drives the
  fan-out, so 15 satellites are 15 schedules and no new code. See ADR 0010.
- **A contributor's failure now fails the whole product run.** The source tier's
  Map is inline, which has no partial-failure tolerance to set: one failed
  iteration fails the tier. Per-source executions were independent, so a GSFC
  failure could not previously block S6. Tolerating it would run the gridded tier over whatever dates landed and
  publish a grid across a hole from a *successful* run; the reference contributors
  have disjoint date ranges, so on any given week only one has data to contribute.
- **`rewrite_manifest` is now `merge_manifests`.** It unions the contributors'
  manifests into one product-tier manifest instead of stamping a published product
  onto a single source's, and **fails the run when two contributors claim the same
  date** rather than letting Map completion order decide which mission's data ships.
  The AWS function `${stage}-rewrite_manifest` is not deleted by the deploy scripts
  and must be removed by hand.
- **Each tier has its own run path**, sharing one `run_id` minted by the new
  `start_run` Lambda. The product tier's manifest moves from
  `pipeline_runs/{source}/{run_id}/{published_product}/jobs.json` to
  `pipeline_runs/{published_product}/{run_id}/jobs.json` — a sibling of its
  contributors' paths rather than a child of one of them. `stage_results_prefix`
  takes `(identity, run_id, stage)` instead of parsing a manifest key to recover an
  identity it then ignored; the leaf state machines' ResultWriter JSONata is
  unchanged, since those three path segments now mean the same thing in both tiers.
- **`pipeline_init` requires a `run_id`** rather than minting one. A locally
  generated one would write to a run path nothing else in the execution reads, and
  the product tier would silently see one fewer contributor.
- **The Run summary is per published product.** `summary.json` moves to
  `pipeline_runs/{published_product}/{run_id}/summary.json`, replaces its top-level
  `source` with `published_product` + `contributors`, names a list of manifests per
  product pipeline, and reconciles the *union* across every contributor on the
  along-track side — reconciling one leg alone would report the others' dates as
  missing on a clean run. It carries a section only for the product pipelines the
  line runs, so a high-latitude satellite's summary holds `along_track` alone. Its
  `schema_version` is now its own (2) rather than `job_outcome.SCHEMA_VERSION`,
  which stays at 1 because Job outcomes are unchanged.
  The success email heads with `Product: NASA-SSH ← GSFC, S6`.
- **Released artifacts now live under a `products/` prefix**, keyed on *published
  product* rather than on source, and separate from the internal staging area
  (`daily_files/`, `crossovers/`, `oer/`, `bad_passes/`, `simple_grids/`,
  `enso_grids/`, `maps/`, `pipeline_runs/`):

  ```
  products/NASA-SSH/along_track/{year}/NASA-SSH_alt_ref_at_v1_1_{YYYYMMDD}.nc
  products/NASA-SSH/simple_grid/{year}/NASA-SSH_alt_ref_simple_grid_v1_1_{YYYYMMDD}.nc
  products/NASA-SSH/indicators/{...}
  products/NASA-SSH_S3B/along_track/{year}/NASA-SSH_S3B_alt_hilat_at_v1_1_{YYYYMMDD}.nc
  ```

  Filenames are unchanged, so moving the existing history is a prefix copy with no
  renaming and no reprocessing. The internal `indicators/{source}/` prefix is gone
  entirely — indicators write through to `products/`, since `indicators.nc` is
  self-read as a cache. `enso_grids/` and `maps/enso_maps/` stay internal (website
  assets), but their path segment is now the published product rather than a source.
  See ADR 0008.

  **Copy the existing `indicators/NASA-SSH/` contents to
  `products/NASA-SSH/indicators/` before the first run.** `load_cached_indicators`
  reads a missing key as "starting fresh", so against an empty prefix the first run
  computes only the current manifest's Mondays, silently skips the 1993
  normalization, and writes a valid-looking GMSL product missing thirty years to a
  PO.DAAC-facing filename — from a successful run.
- **The `unifier` stage is now `publisher`**, and every source runs it. The event is
  `{date, bucket, family, publish_as, contributors}`, with `family` selecting
  along-track or simple-grid promotion, so one Lambda serves both tiers. Still a
  server-side byte copy with no NetCDF engine. The AWS function `${stage}-unifier`
  and the `unifier-sm` state machine are not deleted by the deploy scripts and must
  be removed by hand, or they will keep writing the old addresses. See ADR 0009.
- **`common.unify` is deleted, not renamed.** Publication is derived from
  `product_type` (`reference` → `NASA-SSH`, `high_latitude` → `NASA-SSH_{source}`),
  so there is nothing to gate: the `unifier:` stage section, the `Need Unification?`
  Choice, and the `unify` field on the wire are all gone. `pipeline_init` now returns
  `published_product` in its place, and the finalizer's Job outcome carries
  `published_product` as provenance.
- **`enabled: bool` added to `common`**, defaulting to `false`. Set `true` on GSFC and
  S6; `false` on S6B, S3B and EXAMPLE_S3. This is now the only thing that decides
  whether a source is in production, required because derived publication means a
  product-keyed run would otherwise treat registry membership as production
  membership. A registry test fails if two `enabled` `reference` sources have
  overlapping date ranges, and the publisher refuses a date claimed by more than one
  contributor.
- **`utilities/sources/NASA-SSH.yaml` is deleted.** It was never planned or processed
  — it existed only to lend a directory segment to `daily_file_key`. Published
  identity is now resolved by `utilities/products.py`, and `published_products:` in
  `utilities/products.yaml` declares the facts no source can supply.
- **The simple gridder takes `contributors` + `publish_as` instead of `source`.**
  Contributors are *published products*, read from `products/`; `publish_as` is the
  published product the grid becomes, or `null` for development output. What a grid
  reads and what it becomes are now separate facts, and officialness is declared
  rather than inferred from the contributor set. A `null` `publish_as` resolves to a
  new `simple_grid_development` product (`id: "TBD"`) and writes to
  `simple_grids/development/{identity}/` with a `_dev_` filename family, regardless of
  contributors — deriving the identity from the contributors would have written the
  real reference DOI into a throwaway grid over `["NASA-SSH"]`. `publish_as` must be
  present in the event; absent and `null` are different.
- **The gridded tier is keyed on `published_product` instead of `source`** on the
  wire. `rewrite_manifest`, `set_sg_jobs`, `simple_grids`, `enso` and `indicators`
  all take that field, and the gridded manifest's job specs carry it in place of
  `source`. `run_summary`'s artifact renames `unified_source` to `published_product`,
  the `nasa_ssh_p3` deliverable becomes `published_along_track`, and a second
  `published_simple_grid` deliverable is added; both are unconditional, and each
  tier's promotion reconciles from its own `results/publisher_{family}/` prefix.
- **`xover`'s `reference_source` / `reference_version` become `reference_product`**,
  defaulting to `NASA-SSH`. The reference side is read from
  `products/NASA-SSH/along_track/` rather than `daily_files/p3/NASA-SSH/` — the same
  bytes at a new address. There was never a version to choose (ADR 0006 fixed it at
  P3, and a published along-track file is finalized by definition), so the field is
  gone, along with the `reference_version` global attribute on reference-crossover
  files. Crossovers are an internal cache, so no distributed artifact changes.
- `products.yaml` gains a `published_filename_template` per publishable product,
  taking `{published_name}` where the internal `filename_template` takes `{source}`.
  The two are deliberately distinct so a staging file and a released file can never
  be mistaken for each other; a product with no published template cannot be
  published.
- **`scripts/smoke.sh --source` becomes `--product`**, and `SMOKE_SOURCE` becomes
  `SMOKE_PRODUCT` (`pipeline.sh --smoke`). The pre-check asks the registry which
  Sources contribute to the line and plans each one, since a product-keyed run
  fans out over all of them and any contributor with real work would mutate data.

### Added
- `common.sample_period`, the seconds between a Source's along-track samples,
  defaulting to `1.0` and declared explicitly as `1.0` for GSFC, S6, S6B, and S3B.
  It sets the smoothing filter's tap spacing (see Fixed). Like `ground_speed`, a
  populated value takes a redeploy to reach any stage.
- ERS-1 as a `high_latitude` source (`utilities/sources/ERS1.yaml`), 1992-10-25 through
  1996-06-01 from the AVISO `dataset-l2p-uncross-calibrated-ntc-sla-e1` collection.
  Its constants are measured: `sample_period` 0.980468 s, `ground_speed` 6.7267 km/s,
  and `intermission_bias` 0.0177 m over 1,231,056 reference crossovers. It is
  **disabled** (`enabled: false`, schedule staged with `cron: TBD`) until the
  high-latitude metadata values are settled. The 1993-12-21 to 1994-04-09 ice phase
  has no upstream data and produces empty daily files. The bias against NASA-SSH
  steps by about 11 mm between orbit phases (0.0209 m in the 35-day phases, 0.0102 m
  in the geodetic phase). A single constant is kept because the reference OER fits
  each day's offset regardless of the configured value.
- `tools/compute_ground_speed.py` also measures `sample_period`, as the median
  across passes of each pass's median non-gap time step, and warns when the
  configured value differs.
- **`start_run`**, the Lambda that opens a product-keyed run. Resolves the published
  product's `enabled` contributors, mints the one `run_id` both tiers share, computes
  the product tier's manifest address, decides whether the line has a product tier at
  all, and records the run's invocation parameters. Containerized rather than a zip
  infra Lambda: it exists to read the source registry, which a self-contained zip
  target does not ship.

  **It raises on an empty fan-out**, with distinct messages for a published product no
  registered source resolves to (a typo) and one whose sources are all disabled (an
  `enabled` flag). A run over zero contributors would otherwise succeed having
  produced nothing — indistinguishable downstream from a week with no new upstream
  data.
- `utilities/schedules.yaml` and a paired test: every schedule names a resolvable
  published product with an enabled contributor, and every enabled source's line has
  a schedule. The two failure modes between `enabled` and a schedule are silent in
  opposite directions — an enabled source with no schedule never runs, an enabled
  schedule with no enabled contributor runs and does nothing.

### Removed
- `tools/manifest_generation/unifier_manifest.py`. It read a nonexistent
  `utilities/sources.yaml` and raised `FileNotFoundError` before reaching any of its
  logic, so it could not have been a working reference for the new layout.
- `summarizer`'s published-product-from-jobs-key parser, and the `parts[1]` caveats on
  `jobs_key_identity` and `stage_results_prefix`. All three existed only because the
  gridded manifest nested under the source run path, so every results helper had to
  ignore the identity it was handed.

### Fixed
- `ssha_smoothing` no longer assumes 1 Hz sampling on whole-second timestamps. It
  reindexed each day onto an integer-second grid, which raised `KeyError` for any
  source whose timestamps miss whole seconds (ERS-1, and every other AVISO L2P
  source measured except S3B). Land gaps are now filled by counting omitted samples
  from each gap's length in local sample periods, and filter taps are
  `ground_speed × sample_period` km apart. Output for the 1 Hz sources is
  unchanged. See ADR 0011.
- The AVISO L2P `ssha` comment no longer says AVISO's inter-mission bias is not
  applied. AVISO's `sea_level_anomaly`, which `ssha` is read from unchanged, already
  has `inter_mission_bias` subtracted to tie each mission to TOPEX/Poseidon (verified
  against the documented SLA formula on AltiKa, CryoSat-2, ERS-1, and Jason-1
  granules). `common.intermission_bias` is the residual against the reference
  mission on top of it.
- High-latitude daily files and simple grids no longer ship the NASA-SSH *reference*
  product's in-file identity. `title`, `summary`, `id` (DOI), `processing_level`,
  `product_short_name`, `product_version`, `references`, and `source_url` moved out of
  Python literals in `daily_files/processing/daily_file.py` and
  `simple_gridder/gridding.py` and into per-product entries in `utilities/products.yaml`,
  resolved from the source's `product_type`. Reference-product values are unchanged.
  The high-latitude values are placeholders derived from the reference mission, with
  `id: "TBD"` marking those products as not yet deliverable to PO.DAAC.
  `product_version` is now derived from the product's `version` rather than restated,
  so the filename and the in-file version cannot disagree. `geospatial_lat_min/max`
  remain hardcoded to ±90, which is wrong for every along-track product; unchanged here.
- `pass_flag_notes` in the `finalizer` now follows the source's crossover type. It was
  an unconditional literal describing self crossovers over a 20-day window, so every
  high-latitude file misdescribed its own flagging: those passes are flagged against
  the NASA-SSH reference mission, from each pass's own crossovers, collected over a
  5-day window centered on the processing day. The `reference`-source wording is
  byte-identical to what is already distributed.
- `set_sg_jobs` resolves "today" per invocation. It was bound as a default argument
  and therefore evaluated at module import, so a warm Lambda container kept planning
  simple grids against the date it started on — a container alive for two weeks
  planned two Mondays behind, and dropped the Mondays it had grown past.
- An AVISO host that drops connections now fails the `daily_files` job instead of
  timing out the Lambda. `HttpDownloader` retried connection errors the session's
  adapter had already retried, 30 connect attempts at up to 120 s each, so the job
  never reached the fail-closed path that lets the date replan. Connection errors
  and timeouts now re-raise once the adapter gives up, and the request timeout is
  `(connect=15, read=30)` seconds.

### Added
- Reference-mission crossovers: `high_latitude` sources (e.g. S3B) are now crossed
  against the finalized NASA-SSH P3 reference mission instead of themselves, selected by
  `crossover_type: reference` in a source's `xover` config. The reference SSH is
  time-interpolated to each high-lat crossover time, and the output carries a distinct
  schema (high-lat side + interpolated reference `ssh2` + before/after bracket) and a
  `crossover_type` global attribute. Existing `self` crossovers (S6/GSFC) are unchanged.
  See ADR-0006.
- Reference-crossover OER path: the `oer` stage now reduces orbit error for
  `high_latitude` sources against the reference mission, dispatched from `product_type`.
  It fits a single unstacked `dssh = ssh1 - ssh2` keyed by the high-lat trackid (the
  reference side is fixed truth, not a second observation, so no sign-flipped stacking),
  subtracts a per-source `intermission_bias` before the fit, disables the 0.3 m
  `ssh_max_error` gate, and uses a centered crossover-fetch window. Self-crossover
  output (S6/GSFC) is unchanged.
- Reference-crossover support in the `bad_pass` stage: dispatches self vs reference from
  `product_type` and adds a reference load path (unstacked `dssh = ssh1 - ssh2`, centered
  window) so `high_latitude` sources no longer error. Self-crossover flagging is unchanged.
- Intermission-bias absolute correction in the `finalizer`: for `high_latitude` sources it
  subtracts the per-source `intermission_bias` from `ssha`/`ssha_smoothed`, tying the level
  to the reference datum (idempotent via `intermission_bias_applied`). Adds the S3B
  `finalizer` config. No-op for reference sources.
- High-latitude simple grids: registers the `simple_grid_high_latitude` product so the
  `simple_grids` stage runs on `high_latitude` sources (output `_alt_hilat_simple_grid_`).
- Per-source `intermission_bias` in the `common` section of source configs, consumed by
  the high-latitude OER bias removal and the finalizer datum correction.
- `tools/compute_intermission_bias.py` derives the per-source `intermission_bias`
  constant from a source's L2 files.
- Retry/backoff on the shared AVISO session (`utilities.aviso_auth`): an `HTTPAdapter`
  with exponential backoff + jitter (5 attempts, `backoff_max=60`) retries transient
  connect/read errors and 429/5xx responses, since AVISO's THREDDS server intermittently
  drops connections.

### Changed
- The `xover` `Crossover` god-object is decomposed into composable modules (windowed
  tracks, S3/NetCDF loader, crossover search, results accumulator, and a
  `CrossoverProcessor`/`CrossoverSpec` registry that dispatches on `crossover_type`).
  Self-crossover output is preserved bit-for-bit.
- `S3B` `start_date` moved to `2018-11-24` (from `2018-04-25`).

### Fixed
- Dates with no upstream granules generate empty along-track files again. 
  `pipeline_init` now plans every date from the start of the range through the last date with 
  granules, emitting an empty `granules` list for the gaps, which `daily_files` turns into an 
  empty daily file. Granule-less dates *after* the last granule are still skipped.
- S3B MSS-swap ellipsoid bug: the bundled DTU21 mean-sea-surface grid was on the
  TOPEX/Poseidon ellipsoid while the AVISO L2P SSH is on WGS84, a ~0.68 m offset. Rebuilt
  the grid on WGS84 (`DTU21MSS_1min_WGS84.nc`); no code change. See ADR-0002.
- AVISO L2P (S3B) granule downloads now retry transient body-read/decompress errors (up to 5
  attempts, exponential backoff). A granule still unrecoverable after retries fails the job
  closed rather than writing a partial daily file, so the date self-heals on the next run. See
  issue #41.
- AVISO `inv_bar_cor` is filled with NaN (written as fill) rather than `0.0`.

## [2.4.0] - 2026-09-21

### Added
- Flagged bad-pass counts in the `run_summary` notification, reported within the
  along-track section.
- Per-source `ground_speed` in the `common` section of source configs — one canonical
  value per source, shared by OER (knot placement) and daily-file smoothing.
- `tools/compute_ground_speed.py` derives the per-source `ground_speed` constant from a
  source's L2 files.
- Handler tests for the `oer`, `bad_pass`, `daily_files`, `indicators`, and
  `run_summary` Lambda entry points (previously untested), plus `utilities.encoding`.

### Changed
- The `daily_file` Distributed Map now sets `ToleratedFailurePercentage: 5`, so a
  small fraction of failing items no longer aborts the whole map run.
- Updated the `simple_grids` basin connection table (fixed asymmetric connections,
  removed a duplicate, pruned spurious links across 12 basins), versioned it in the
  filename (`basin_connection_table_v2.txt`), and added an `HDR` header matching the
  NASA-SSH PO.DAAC reference-file convention. The loader now skips the header.
- Renamed the GSFC `dac`/`inv_bar_cor` source flavors to describe their contents:
  `GSFC_6.1_IB_APPLIED` and `GSFC_6.1_NO_ATMOS`.
- Tidied the GSFC/S6/AVISO ingestors: opened datasets are now closed deterministically
  and removed unused in-memory data.

### Fixed
- OER now skips cleanly (`status: skipped`) when a date's p1 daily file does not exist,
  instead of failing the zero-tolerance OER map. A `daily_file` job can fail and be
  absorbed by its `ToleratedFailurePercentage`, leaving no p1 to correct.
- `key_exists()` checks could report a file missing after it had landed: `s3fs` caches
  parent-directory listings forever by default, so a check that 404'd before an upstream
  file appeared poisoned that directory's listing across warm-Lambda invocations. Disabled
  the listing cache (`use_listings_cache=False`) so each check issues a fresh per-key
  `head_object`.
- GSFC ingest no longer silently emits corrupt corrections.

## [2.3.0] - 2026-07-15

### Added
- Post-deploy verification: `scripts/prod/verify.sh <version>` asserts every
  deployable target's live Lambda settled (State=Active, LastUpdateStatus=
  Successful) and runs the shipped image (`<repo>:<version>`). `release.sh` runs
  it after the Lambda deploy; it's also runnable standalone to audit prod (e.g.
  to confirm a half-applied release was reconciled). State-machine deploys now
  verify the live definition matches what was rendered (canonical JSON compare).
- `scripts/smoke.sh --source <S> --start <d> --end <d> [--stage]` — a
  non-mutating end-to-end smoke test that runs an already-processed window with
  no force_update (fast-exits to run_summary, writes no product data) and asserts
  the run summary reconciles clean. A pre-check refuses windows with real work.
- `scripts/state_machines/rollback.sh --version <X.Y.Z> --stage <s>` re-deploys a
  released version's state machine definitions by shallow-cloning that git tag,
  rendering it, and deploying with current tooling. Step Functions overwrites
  definitions in place (no per-version artifact like Lambda images in ECR), so
  the tag is the version-keyed source of truth. The working tree is untouched.
- State-machine deploys can stamp a `version` resource tag (`deploy.sh --version`)
  so the live release is queryable when deciding what to roll back to. The
  release pipeline stamps the release version; the dev pipeline stamps the git sha.
- `scripts/test.sh` runs the test suite one pytest process per stage, each rooted
  at the stage directory so stage code resolves as top-level imports (mirroring
  the Lambda container). Accepts stage selectors and pytest passthrough args, and
  prints a per-stage PASS/FAIL summary. Stage list comes from the Target registry.
- `ruff` linting: added to the `dev` extra with config in `pyproject.toml`, plus a
  fast `lint` gate in CI (`astral-sh/ruff-action`, version pinned to `uv.lock`).

### Changed
- **Dependencies consolidated into `pyproject.toml`** as the single source of
  truth, replacing `setup.py` and all 12 per-stage `requirements.txt` files. Each
  stage is a named optional extra; Dockerfiles install via `uv` (export the locked
  deps for the stage, then install the shared package with `--no-deps`), preserving
  the layer-cache split so a shared-package edit doesn't rebuild the scientific
  stack. `uv.lock` pins the full dependency graph.
- CI now runs a single job — `uv sync --extra dev` + `scripts/test.sh` — replacing
  the per-module matrix that read the now-deleted `requirements.txt` / `setup.py`.
- The build/deploy pipelines now render and deploy the Step Functions state
  machine definitions (`state_machines/*.asl.json`) alongside the Lambdas, after
  the Lambda deploy. `scripts/prod/release.sh` always ships them; the dev
  `scripts/dev/pipeline.sh` deploys them when an ASL template changed vs the base
  (or with `--all`). Previously this was a separate manual step, so an ASL change
  could ship without the orchestration that drives it (or vice versa).
- `scripts/dev/pipeline.sh --smoke` runs the non-mutating smoke test after a dev
  deploy, with source/window from the environment (`SMOKE_SOURCE` / `SMOKE_START`
  / `SMOKE_END`). Opt-in, so routine dev pushes don't start an execution.

### Fixed
- Dependency drift the split `requirements.txt` / extras sources had hidden:
  `requests` was used by `daily_files` but undeclared (resolved only transitively),
  and `simple_grids` pinned `requests==2.25.1` — unused, and in conflict with
  `python-cmr`'s `requests>=2.26.0`. Declared the former, dropped the latter.
- The Target registry (`utilities/targets.py`) now locates the repo root and its
  shared-build-path edge via `pyproject.toml` instead of the deleted `setup.py`.
- Removed vestigial stage-root `__init__.py` files (`daily_files`, `pipeline_init`)
  that shadowed the inner packages and broke test collection.
- Container Lambda deploys now wait for the code update to settle
  (`lambda wait function-updated-v2`) instead of fire-and-forget, so a failed
  image update fails the deploy at the deploy step. Previously only zip Lambdas
  waited.
- Production releases no longer fail trying to deploy a nonexistent
  `prod-podaac_auth`. The Target registry now supports stage-agnostic singletons
  via `function` (explicit Lambda name) and `deploy_stages` (which stages deploy
  it): the shared `podaac_cred_update` credential refresher is deployed under its
  real name, from prod only.
- enso image failed to build: `pillow` (transitive via `matplotlib`) resolved to
  12.x, which ships no glibc-2.26 wheel, so it fell back to a source build with no
  compiler in the image. Pinned `pillow==11.3.0` (last release with a
  manylinux_2_17 wheel) and re-locked.

## [2.2.0] - 2026-06-24

### Added
- **Run summary & job-outcome contract.** Deliverable stages now return a declared
  `JobOutcome`; a reconciling `run_summary` Lambda reads those outcomes against each
  run's manifest and writes a `pipeline_runs/{source}/{run_id}/summary.json` artifact,
  then renders the success notification from it instead of inferring success from live
  S3 listings. Adds a `processing_history` provenance attribute carried through the
  file-writing stages. See ADR 0005. (#34)
- **Fast exit for empty runs.** When `pipeline_init` finds no dates to process, the
  along-track and orchestrator state machines skip all compute (daily-file → finalizer,
  unification, simple-grids) and go straight to `run_summary`, which emits an empty
  (`expected 0 / produced 0`) summary and notification. (#34)
- **SNS failure notifications.** Pipeline failures are surfaced via SNS with grouped,
  de-duplicated failure detail. See ADR 0003. (#29)
- **Target registry** (`utilities/targets.py` + `targets.yaml`) as the single source of
  truth for build/deploy, deriving packaging kind (container image vs zip) from the
  filesystem and branching the build/deploy scripts on it. See ADR 0004. (#32)
- **AVISO L2P processor** for the high-latitude source. (#27)
- **PR-time CI gate** running unit tests across all along-track modules. (#26)

### Changed
- `pipeline_runtime` base image plus source/discovery/layout refactors (AVISO Phase 1). (#25)
- `failure_handling` reverted to failure-only; its success path is superseded by
  `run_summary`. (#34)

### Fixed
- `set_sg_jobs` no longer crashes with `min() iterable argument is empty` on no-new-files
  runs; an empty manifest now yields an empty `sg_jobs.json`. (#34)
- Deploy scripts no longer block on the AWS CLI v2 pager after each
  `update-function-code` / `update-state-machine` call. (#34)
- Bucket name resolution when running via the scheduler.
- Incorrect daily-file filename pattern.

## Prior releases

Releases before 2.2.0 predate this changelog; see the git tags for their contents:
[v2.1.0], [v2.0.0], [v1.1.0].

[Unreleased]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.4.0...HEAD
[2.4.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.1.0...v2.2.0
[v2.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.1.0
[v2.0.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.0.0
[v1.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v1.1.0
