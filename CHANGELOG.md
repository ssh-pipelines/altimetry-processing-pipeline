# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.2.0...HEAD
[2.2.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/compare/v2.1.0...v2.2.0
[v2.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.1.0
[v2.0.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v2.0.0
[v1.1.0]: https://github.com/ssh-pipelines/altimetry-processing-pipeline/releases/tag/v1.1.0
