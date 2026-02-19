# ALTIMETRY PROCESSING PIPELINE

Monorepo for the independent stages of the altimetry processing pipeline used to generate NASA SSH products. 

Pipeline consists of:

### Daily along track generation

Daily files -> Crossover -> OER -> Crossover -> Bad pass flagging -> Finalization

### Additional product generation

Simple grids -> Indicators -> Imagery for website

```
                  GSFC Data     S6 Data
                      |            |
                      +------------+
                            |
                            v
                [ Generate Daily Files ]
                            |   
                            v   
                [ Generate Simple Grids ]
                            |  
                            v  
             +--------------+--------------+ 
             |                             | 
             v                             v 
[ Generate ENSO Maps & Imagery]  [ Generate Indicators ]
```

## Description

The directories contained within `pipeline/` contain everything for a given stage in the pipeline (note: stage as opposed to step, as the crossover stage is used for two steps). Each stage's directory can be thought of as an independent repository for that stage.

Each stage has been designed to be run on AWS Lambda, and as such there are limitations to how much can be executed locally.

## Step Function Architecture

The pipeline is orchestrated by AWS Step Functions using JSONata query language. State machine definitions live in `state_machines/` as ASL (Amazon States Language) JSON files. The `rendered/` subdirectory contains the same definitions with AWS account/region placeholders resolved.

### Hierarchy

A top-level orchestrator (`pipeline.asl.json`) coordinates three sub-pipelines, each of which delegates per-date work to Distributed Map states that fan out to Lambda functions:

```
pipeline.asl.json
├── along_track_pipeline.asl.json
│   ├── pipeline_init (Lambda — determines which dates need processing)
│   ├── daily_file.asl.json (Distributed Map → daily_files Lambda)
│   ├── xover.asl.json (Distributed Map → xover Lambda, df_version=p1)
│   ├── oer.asl.json (Distributed Map → oer Lambda)
│   ├── xover.asl.json (Distributed Map → xover Lambda, df_version=p2)
│   ├── bad_pass.asl.json (Distributed Map → bad_pass Lambda)
│   └── finalizer.asl.json (Distributed Map → finalizer Lambda)
├── unifier.asl.json (conditional — runs if source has unify=true)
│   ├── Distributed Map → unifier Lambda
│   └── rewrite_manifest (Lambda — rewrites jobs manifest with NASA-SSH source)
└── simple_grid_pipeline.asl.json
    ├── set_sg_jobs (Lambda — filters manifest to Monday dates)
    ├── simple_grid.asl.json (Distributed Map → simple_grids Lambda)
    ├── enso.asl.json (Distributed Map → enso Lambda)
    └── indicators (Lambda)
```

### Key patterns

- **Jobs manifest**: `pipeline_init` writes a `jobs.json` to S3 listing dates that need processing. All downstream Distributed Maps read from this manifest via `ItemReader`.
- **Distributed Map**: Most processing stages use Distributed Map (mode `DISTRIBUTED`, max concurrency 100) to process each date independently. Results are written to S3 via `ResultWriter`.
- **Input threading**: Orchestrator states use `Output: "{% $states.input %}"` to pass the original input (`jobs_key`, `bucket`, `source`) through to the next state, since child state machine outputs aren't needed upstream.
- **Two crossover passes**: The xover state machine is invoked twice — once with `df_version=p1` (before OER) and once with `df_version=p2` (after OER).
- **Conditional unification**: Sources with `unify=true` (GSFC, S6) get their finalized daily files copied to a unified `NASA-SSH` prefix by the unifier. The `rewrite_manifest` Lambda then produces a new jobs manifest under the NASA-SSH source for downstream simple grid processing.

For a detailed reference of every S3 object written by each stage (success and failure), key patterns, and operator troubleshooting tips, see **[S3_DATA_FLOW.md](S3_DATA_FLOW.md)**.

## Source Configuration

Source-specific settings are split across two layers:

### Global registry: `utilities/sources.yaml`

Shared metadata used by multiple pipeline stages. Fields:

- `product_type` — `reference` or `hilat`; determines filename conventions
- `unify` — whether the source participates in NASA-SSH unification
- `start_date` — first date with available data
- `end_date` (optional) — last date with available data; omit for ongoing collections

Example:
```yaml
sources:
  GSFC:
    product_type: reference
    unify: true
    start_date: "1992-10-25"
    end_date: "2024-01-20"
  S6:
    product_type: reference
    unify: true
    start_date: "2024-01-21"
```

### Stage-local configs: `<stage>/config/sources.yaml`

Stage-specific settings that only apply to a single Lambda. For example, `pipeline_init/config/sources.yaml` contains CMR collection IDs, S3 prefixes, and filename patterns that are only relevant to the init stage. Each stage's config loader merges its local fields with the global registry.

When adding a new source, add an entry to `utilities/sources.yaml` first, then add stage-specific entries to each stage's local config as needed.

## Setup

### Installing `utilities`:
Root level `utilities` module is required to be installed in order to successfully execute containers and unit tests.

From the root directory:
```
python -m pip install .
```

### Building images:

Images must be built from the root directory context

ex:
```
docker buildx build --platform linux/amd64 --load -t daily_files:latest -f path/to/nasa-ssh-pipeline/pipeline/daily_files/Dockerfile {path/to/nasa-ssh-pipeline/}
```