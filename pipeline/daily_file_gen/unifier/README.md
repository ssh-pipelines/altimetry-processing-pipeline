# Unifier

Copies finalized P3 daily files from per-source S3 paths to a unified `NASA-SSH` prefix, producing the combined NASA Sea Surface Height product. Only sources with `unify=true` in the global registry participate (currently GSFC and S6). Runs as an AWS Lambda function, invoked once per date via a Distributed Map in the `unifier.asl.json` Step Function.

## How it works

For each processing date, the Lambda:

1. **Loads the source config** from `config/sources.yaml` to determine source and destination filename templates and the destination S3 prefix.
2. **Copies the P3 daily file** from the per-source path to the unified NASA-SSH path using `s3.copy_object`. No data transformation occurs — this is a server-side S3 copy.
3. **Returns** the original event with `source` replaced by `"NASA-SSH"`, signaling to downstream stages that the unified product is ready.

After the Distributed Map completes, the Step Function invokes a separate `rewrite_manifest` Lambda that rewrites the jobs manifest with `source: "NASA-SSH"` for downstream simple grid processing.

## Directory structure

```
unifier/
├── app.py                          # Lambda handler (S3 copy logic)
├── config/
│   ├── __init__.py
│   ├── sources.yaml                # Per-source filename templates and destination prefix
│   └── source_config.py            # Dataclasses + YAML loader (lazy-cached)
├── tests/
│   ├── __init__.py
│   └── test_unifier.py             # Unit tests (config loading, handler copy, error cases)
├── Dockerfile
├── requirements.txt
└── README.md
```

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6"
}
```

All three fields are required. The `source` must be configured in `config/sources.yaml` — unconfigured sources (e.g., S6B) raise a `ValueError`.

## Lambda output

```json
{
  "status": "success",
  "data": {
    "bucket": "my-bucket",
    "date": "2025-01-15",
    "source": "NASA-SSH"
  }
}
```

Note that `source` is replaced with `"NASA-SSH"` in the output.

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/p3/{source}/{year}/{source}_alt_ref_at_v1_{YYYYMMDD}.nc` | Source P3 daily file (read) |
| `daily_files/p3/NASA-SSH/{year}/NASA-SSH_alt_ref_at_v1_{YYYYMMDD}.nc` | Unified NASA-SSH daily file (write) |

## Source configuration

Unlike other stages, the unifier config is fully self-contained — it does not merge with the global source registry. Only sources that should participate in NASA-SSH unification are listed.

| Field                    | Description                                                    |
|--------------------------|----------------------------------------------------------------|
| `src_filename_template`  | Source filename with `{source}` and `{date8}` placeholders     |
| `dst_filename_template`  | Destination filename (always `NASA-SSH` prefixed)              |
| `dst_prefix`             | Destination S3 prefix (e.g., `daily_files/p3/NASA-SSH`)        |

Current sources:

| Source | Destination Prefix       | Notes |
|--------|--------------------------|-------|
| GSFC   | `daily_files/p3/NASA-SSH`| Unified into NASA product |
| S6     | `daily_files/p3/NASA-SSH`| Unified into NASA product |

S6B is intentionally omitted — it is not unified into the NASA product yet.

## Step Function

Defined in `state_machines/unifier.asl.json`. Contains two states:

1. **Distributed Map** (max concurrency 500) — reads dates from the jobs manifest and invokes the `unifier` Lambda for each date. Results are written to `pipeline_runs/results/unifier/` in S3.
2. **Rewrite Manifest** — invokes the `rewrite_manifest` Lambda, which rewrites the jobs manifest with `source: "NASA-SSH"` so downstream stages (simple grids, ENSO, indicators) process the unified product.

The unifier step function runs conditionally — only when the source has `unify=true` in the global registry.

## Running tests

From the `unifier/` directory:

```bash
source .venv/bin/activate  # or use the devcontainer
python -m unittest discover -s tests -t . -v
```

### Test coverage

| Test class                      | What it tests                                                    |
|---------------------------------|------------------------------------------------------------------|
| `TestSourceConfig`              | Available sources include GSFC/S6, S6B excluded, config values, invalid/unconfigured source raises |
| `TestUnifierHandler`            | GSFC and S6 both copy to NASA-SSH path with correct src/dst keys |
| `TestUnifierSkipsUnconfigured`  | Unconfigured source (S6B) raises ValueError, missing params raises ValueError |

## Dependencies

Key libraries (see `requirements.txt`):

- `boto3` — S3 copy operations
- `pyyaml` — source config loading
