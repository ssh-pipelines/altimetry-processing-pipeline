# ALTIMETRY PROCESSING PIPELINE

[![tests](https://github.com/ssh-pipelines/altimetry-processing-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/ssh-pipelines/altimetry-processing-pipeline/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/ssh-pipelines/altimetry-processing-pipeline/branch/main/graph/badge.svg)](https://codecov.io/gh/ssh-pipelines/altimetry-processing-pipeline)

Monorepo for the independent stages of the altimetry processing pipeline used to generate NASA SSH products. 

Pipeline consists of:

### Daily along track generation

Daily files -> Crossover -> OER -> Crossover -> Bad pass flagging -> Finalization

Every source runs its own independent instance of this sequence, writing into
internal staging under `daily_files/{p1,p2,p3}/{source}/`. Every source then runs
the **publisher**, which promotes its finalized daily file to the released address
under `products/{published product}/`. Which published product a source feeds is
derived from its `product_type` — `reference` sources all collapse into the one
`NASA-SSH` line, each `high_latitude` satellite publishes its own
`NASA-SSH_{source}` line — so there is no publish gate to configure
([ADR 0008](docs/adr/0008-published-products-prefix.md),
[ADR 0009](docs/adr/0009-publisher-stage-and-enabled.md)).

### Additional product generation

Simple grids -> ENSO grids & imagery -> Indicators

### Documentation

- **[docs/DATA_FLOW.md](docs/DATA_FLOW.md)** — the end-to-end picture as a diagram:
  which upstream collections are ingested from where, and what is published at each
  level.
- **[docs/GLOSSARY.md](docs/GLOSSARY.md)** — the vocabulary this repo is written in
  (source, granule, daily file, P1/P2/P3, crossover type, product, published
  product, staging, contributor, run summary). Worth skimming before a first PR.
- **[docs/adr/](docs/adr/)** — architecture decision records: why the scientific
  stack is pinned in one image, why AVISO L2P skips the MSS swap, how failures are
  surfaced, and more.

## Description

The directories contained within `pipeline/` contain everything for a given stage in the pipeline (note: stage as opposed to step, as the crossover stage is used for two steps). Each stage's directory can be thought of as an independent repository for that stage.

Each stage has been designed to be run on AWS Lambda, and as such there are limitations to how much can be executed locally.

## Step Function Architecture

The pipeline is orchestrated by AWS Step Functions using JSONata query language. State machine definitions live in `state_machines/` as ASL (Amazon States Language) JSON files. The `rendered/` subdirectory contains the same definitions with AWS account/region placeholders resolved.

### Hierarchy

A **trigger names a Published product**, not a Source. The top-level orchestrator
(`pipeline.asl.json`) composes two optional tiers over it: a *source tier* Mapped
across the contributing sources, and a *product tier* for the gridded products.
Each tier delegates per-date work to Distributed Map states that fan out to Lambda
functions.

```
pipeline.asl.json                 ← trigger: {published_product, bucket}
├── start_run (Lambda — enabled contributors, run_id, whether there is a product tier)
├── Source Tier (inline Map over contributors)        [keyed on Source]
│   ├── at_pipeline.asl.json
│   │   ├── pipeline_init (Lambda — determines which dates need processing)
│   │   ├── daily_file.asl.json (Distributed Map → daily_files Lambda)
│   │   ├── xover.asl.json (Distributed Map → xover Lambda, df_version=p1)
│   │   ├── oer.asl.json (Distributed Map → oer Lambda)
│   │   ├── xover.asl.json (Distributed Map → xover Lambda, df_version=p2)
│   │   ├── bad_pass.asl.json (Distributed Map → bad_pass Lambda)
│   │   └── finalizer.asl.json (Distributed Map → finalizer Lambda)
│   └── publisher.asl.json (family=along_track — promotes each P3 to products/)
│       └── Distributed Map → publisher Lambda
├── merge_manifests (Lambda — unions the contributors' dates into one manifest)
├── sg_pipeline.asl.json                              [keyed on Published product]
│   ├── set_sg_jobs (Lambda — filters manifest to Monday dates)
│   ├── simple_grids.asl.json (Distributed Map → simple_grids Lambda)
│   ├── publisher.asl.json (family=simple_grid — promotes each grid to products/)
│   ├── enso.asl.json (Distributed Map → enso Lambda)
│   └── indicators (Lambda)
└── run_summary (Lambda — reconciles expected vs produced, per product pipeline)
```

Two live shapes. A `{published_product: "NASA-SSH"}` run resolves to the enabled
reference sources (GSFC and S6 today), Maps the source tier across them, merges their
manifests, and runs the product tier. A `{published_product: "NASA-SSH_S3B"}` run
resolves to one contributor and has **no** product tier — at most one high-latitude
grid exists and it is composed from every satellite, so ENSO and indicators are
unreachable from a per-satellite trigger rather than merely unused. `product_type`
drives the fan-out, so 15 high-latitude satellites are 15 schedules and no new code.

`merge_manifests` is where the run changes key: everything above it is keyed on a
**Source**, everything below on the **Published product** those Sources feed. Each
tier's manifest and ResultWriter outputs live under its own run path
(`pipeline_runs/{identity}/{run_id}/`), sharing the one `run_id` `start_run` minted,
so a run is traceable end to end without either path nesting inside the other.

A contributor whose along-track run finds no dates short-circuits past its own
publication; a run where no contributor planned anything skips the gridded tier
entirely and reconciles to an empty run rather than a failure.

### Key patterns

- **Jobs manifest**: `pipeline_init` writes a `jobs.json` to S3 listing dates that need processing. All downstream Distributed Maps read from this manifest via `ItemReader`.
- **Distributed Map**: Most processing stages use Distributed Map (mode `DISTRIBUTED`, max concurrency 100) to process each date independently. Results are written to S3 via `ResultWriter`.
- **Input threading**: Orchestrator states use `Output: "{% $states.input %}"` (or `$merge` onto it) to carry the run envelope (`published_product`, `bucket`, `run_id`, `contributors`, `jobs_key`) through to the next state, since child state machine outputs aren't needed upstream.
- **Two crossover passes**: The xover state machine is invoked twice — once with `df_version=p1` (before OER) and once with `df_version=p2` (after OER).
- **Unconditional publication**: every source's finalized daily file is promoted to `products/{published product}/along_track/` by the publisher, and every staged simple grid to `products/{published product}/simple_grid/`. One publisher Lambda serves both, with `family` as an argument; its ResultWriter prefix carries the family so the two tiers' outcomes stay separate.
- **Staging vs `products/`**: `daily_files/`, `crossovers/`, `oer/`, `bad_passes/`, `simple_grids/`, `enso_grids/`, `maps/` and `pipeline_runs/` are internal working state, reproducible from upstream. `products/` holds the released artifacts, and every key under it comes from one `published_*` function family in `pipeline_layout` so a later move to a separate bucket is one module plus a config value.
- **Loud empty fan-outs**: a trigger naming a published product with no `enabled` contributor fails in `start_run`, and two contributors claiming one date fails in `merge_manifests`. Both would otherwise be a run that succeeds having produced nothing, or one where completion order picks what ships.
- **Declared outcomes, not inferred ones**: each deliverable stage returns a Job outcome naming the S3 keys it wrote, persisted per item by `ResultWriter`. `run_summary` reconciles those against the manifest's job specs rather than listing S3 and matching date tokens ([ADR 0005](docs/adr/0005-job-outcome-contract-and-run-summary.md)). Failures take the mirrored path through `failure_handling` ([ADR 0003](docs/adr/0003-failure-surfacing.md)).

The authoritative reference for every S3 key each stage reads and writes is
[`utilities/pipeline_layout.py`](utilities/pipeline_layout.py) — every Lambda asks
that module for keys and prefixes rather than building f-strings.

## Source Configuration

Each source has one profile file, `utilities/sources/{source}.yaml`, holding a
shared `common:` block plus one section per pipeline stage. A stage's config
loader (`load_source_config`) merges `common` with that stage's section and
constructs the stage's config dataclass, so each stage sees only the fields it
declares.

### Shared block: `common:`

Source-identity metadata used by multiple stages:

- `product_type` — `reference` or `high_latitude`; determines the daily-file
  filename family (`alt_ref_at_*` vs `alt_hilat_at_*`) and which downstream
  stages apply
- `discovery_type` — how pipeline_init finds upstream granules (`cmr`, `thredds`, …)
- `enabled` — whether the source is in production. Declared, not derived: registry
  membership is not production membership. Defaults to `false`, so a source can be
  staged in config ahead of its schedule
- `start_date` — first date with available data
- `end_date` (optional) — last date with available data; omit for ongoing collections
- `collections` — the upstream collection descriptors discovery reads

### Stage sections

Each stage that a source participates in gets its own section (`pipeline_init:`,
`daily_files:`, `finalizer:`, `xover:`, …) carrying only that stage's settings.

The `xover:` section selects the **crossover type**:

- `crossover_type: self` — a `reference` product-type source (S6, S6B, GSFC)
  crossed against its own passes over a forward window. Fields: `cycle_length`,
  `window_size`, `window_padding`, `max_pass_number`.
- `crossover_type: reference` — a `high_latitude` source (S3B) crossed against
  the published NASA-SSH reference along-track product over a window *centered* on
  the processing day. Optionally overrides `reference_product` (see ADR-0006).

Example (`utilities/sources/S3B.yaml`, abbreviated):
```yaml
common:
  product_type: high_latitude
  discovery_type: thredds
  enabled: false
  start_date: "2018-04-25"

xover:
  crossover_type: reference
  reference_product: NASA-SSH
  cycle_length: 9.9156
  window_size: 12      # interpreted as a ±centered window for reference crossovers
  window_padding: 2
  max_pass_number: 9999
```

When adding a new source, create `utilities/sources/{source}.yaml` with a
`common` block and a section for each stage it participates in.

## Setup

Dependencies are declared in the root `pyproject.toml` (the single source of
truth) and locked in `uv.lock`. Each stage is a named optional extra; the `dev`
extra installs the union of them all plus the test/lint tooling. Use
[uv](https://docs.astral.sh/uv/) to create the dev environment:

```
uv sync --extra dev
```

This installs the shared `utilities` package (required to execute containers and
unit tests) along with every stage's dependencies.

### Running tests:

Tests run one process per stage via `scripts/test.sh` — each stage is rooted at
its own directory so its code resolves as a top-level import, mirroring the
Lambda container. See the script header for the rationale.

```
./scripts/test.sh              # all stages
./scripts/test.sh oer xover    # only the named stages
./scripts/test.sh oer -- -k foo -x   # pass args after `--` through to pytest
```

### Linting:

```
uv run ruff check .            # config lives in [tool.ruff] in pyproject.toml
```

### Building images:

Images must be built from the root directory context. Each stage Dockerfile
exports its extra's locked dependencies with `uv` and installs the shared
`utilities` package, so no per-stage `requirements.txt` is needed.

ex:
```
docker buildx build --platform linux/amd64 --load -t daily_files:latest -f path/to/nasa-ssh-pipeline/pipeline/daily_files/Dockerfile {path/to/nasa-ssh-pipeline/}
```