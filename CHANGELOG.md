# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Flagged bad-pass counts in the `run_summary` notification, reported within the
  along-track section.
- Per-source `ground_speed` and `intermission_bias` in the `common` section of source
  configs — one canonical value per source, shared by OER (knot placement, bias
  removal) and daily-file smoothing.
- `tools/compute_ground_speed.py` and `tools/compute_intermission_bias.py` derive the
  per-source constants above from a source's L2 files.
- Handler tests for the `oer`, `bad_pass`, `daily_files`, `indicators`, and
  `run_summary` Lambda entry points (previously untested), plus `utilities.encoding`.
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
- The `daily_file` Distributed Map now sets `ToleratedFailurePercentage: 5`, so a
  small fraction of failing items no longer aborts the whole map run.
- Updated the `simple_grids` basin connection table (fixed asymmetric connections,
  removed a duplicate, pruned spurious links across 12 basins), versioned it in the
  filename (`basin_connection_table_v2.txt`), and added an `HDR` header matching the
  NASA-SSH PO.DAAC reference-file convention. The loader now skips the header.

### Fixed
- S3B MSS-swap ellipsoid bug: the bundled DTU21 mean-sea-surface grid was on the
  TOPEX/Poseidon ellipsoid while the AVISO L2P SSH is on WGS84, a ~0.68 m offset. Rebuilt
  the grid on WGS84 (`DTU21MSS_1min_WGS84.nc`); no code change. See ADR-0002.
- OER now skips cleanly (`status: skipped`) when a date's p1 daily file does not exist,
  instead of failing the zero-tolerance OER map. A `daily_file` job can fail and be
  absorbed by its `ToleratedFailurePercentage`, leaving no p1 to correct.
- Reference-crossover jobs intermittently reported "No daily file … skipping" for files
  that existed: `s3fs` caches parent-directory listings forever by default, so a
  `key_exists()` check that 404'd before an upstream file landed poisoned the listing for
  that whole directory across warm-Lambda invocations. Disabled the listing cache
  (`use_listings_cache=False`) so each check issues a fresh per-key `head_object`.
- AVISO L2P (S3B) granule downloads now retry transient body-read/decompress errors (up to 5
  attempts, exponential backoff). A granule still unrecoverable after retries fails the job
  closed rather than writing a partial daily file, so the date self-heals on the next run. See
  issue #41.

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

[Unreleased]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.3.0...HEAD
[2.3.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.1.0...v2.2.0
[v2.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.1.0
[v2.0.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.0.0
[v1.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v1.1.0
