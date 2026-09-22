# Unifier

Copies finalized P3 daily files from per-source S3 paths to a unified `NASA-SSH` prefix, producing the combined NASA Sea Surface Height product. Only reference-mission sources with `unify: true` in their source profile participate (currently GSFC and S6); high-latitude sources publish their own product and never pass through here. Runs as an AWS Lambda function, invoked once per date via a Distributed Map in the `unifier.asl.json` Step Function.

## How it works

For each processing date, the Lambda:

1. **Loads the source config** from `utilities/sources/{source}.yaml` and the `NASA-SSH` identity profile, then derives both the source and destination P3 keys from `utilities.pipeline_layout` (`daily_file_key`). No filenames are configured here.
2. **Copies the P3 daily file** from the per-source path to the unified NASA-SSH path using `s3.copy_object`. No data transformation occurs — this is a server-side S3 copy. The source file's `processing_history` attribute rides along in the copy unchanged (the unifier does not open the NetCDF).
3. **Returns** a **Job outcome** declaring the `nasa_ssh_p3` key it wrote. Because it never opens the file, it omits `provenance_complete` (the byte-identical along-track P3 outcome carries the authoritative flag); `run_summary` reports that as "unknown".

After the Distributed Map completes, the Step Function invokes a separate `rewrite_manifest` Lambda that rewrites the jobs manifest with `source: "NASA-SSH"` for downstream simple grid processing.

## Directory structure

```
unifier/
├── app.py                          # Lambda handler (S3 copy logic)
├── config/
│   ├── __init__.py
│   └── source_config.py            # Binds the shared source profile to this stage
├── tests/
│   ├── __init__.py
│   └── test_unifier.py             # Unit tests (config loading, handler copy, error cases)
├── Dockerfile
└── README.md
```

Per-source settings live in `utilities/sources/{source}.yaml`, not in this directory.

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-15",
  "source": "S6"
}
```

All three fields are required. The `source` must have an `unifier:` section in `utilities/sources/{source}.yaml` — sources without one (e.g. S6B) raise a `ValueError`.

## Lambda output

A **Job outcome** (`JobOutcome.to_dict()`) declaring the unified key:

```json
{
  "schema_version": 1,
  "stage": "unifier",
  "status": "success",
  "date": "2025-01-15",
  "source": "NASA-SSH",
  "outputs": [
    {"key": "daily_files/p3/NASA-SSH/2025/NASA-SSH_alt_ref_at_v1_1_20250115.nc", "kind": "nasa_ssh_p3"}
  ],
  "metadata": {"copied_from": "daily_files/p3/S6/2025/S6_alt_ref_at_v1_1_20250115.nc"}
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/p3/{source}/{year}/{source}_alt_ref_at_v1_1_{YYYYMMDD}.nc` | Source P3 daily file (read) |
| `daily_files/p3/NASA-SSH/{year}/NASA-SSH_alt_ref_at_v1_1_{YYYYMMDD}.nc` | Unified NASA-SSH daily file (write) |

## Source configuration

Like every other stage, the unifier reads `utilities/sources/{source}.yaml` and merges
`common` with its own stage section. **It declares no stage-specific fields** —
`UnifierSourceConfig` is just `SourceCommon`, because both the source and destination
keys are derived from `utilities.pipeline_layout` rather than configured. The
`unifier:` section exists purely as the opt-in marker.

Two independent things gate unification, and both must agree for a source to be unified:

| Gate | Where | What it controls |
|------|-------|------------------|
| `common.unify: true` | source YAML | Whether the top-level state machine runs the unifier at all (the `Need Unification?` Choice tests `unify = true`) |
| an `unifier:` section | source YAML | Whether the Lambda accepts the source; absent ⇒ `ValueError` |

Current sources (both gates set):

| Source | `unify` | `unifier:` section | Destination |
|--------|---------|--------------------|-------------|
| GSFC   | `true`  | yes                | `daily_files/p3/NASA-SSH/` |
| S6     | `true`  | yes                | `daily_files/p3/NASA-SSH/` |

S6B is intentionally omitted from both — it is not unified into the NASA product yet.
The destination filename comes from the `NASA-SSH` identity profile, whose
`product_type: reference` selects the `alt_ref_at_*` family.

## Step Function

Defined in `state_machines/unifier.asl.json`. Contains two states:

1. **Distributed Map** (max concurrency 500) — reads dates from the jobs manifest and invokes the `unifier` Lambda for each date. The invoke task unwraps the Lambda result (`Output: {% $states.result.Payload %}`) so the **Job outcome** is what the `ResultWriter` persists under `pipeline_runs/{source}/{run_id}/results/unifier/` for `run_summary` to read. *(This unwrap was missing originally, so `run_summary` saw the raw Lambda envelope and reported `produced: 0` — see ADR 0005 / the run_summary README.)*
2. **Rewrite Manifest** — invokes the `rewrite_manifest` Lambda, which rewrites the jobs manifest with `source: "NASA-SSH"` so downstream stages (simple grids, ENSO, indicators) process the unified product.

The unifier step function runs conditionally — only when the source has `unify: true` in its source profile.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh unifier
```

### Test coverage

| Test class                      | What it tests                                                    |
|---------------------------------|------------------------------------------------------------------|
| `TestSourceConfig`              | Available sources include GSFC/S6, S6B excluded, config values, invalid/unconfigured source raises |
| `TestUnifierHandler`            | GSFC and S6 both copy to NASA-SSH path with correct src/dst keys |
| `TestUnifierJobOutcome`         | The handler declares the `nasa_ssh_p3` output in its Job outcome |
| `TestUnifierSkipsUnconfigured`  | Unconfigured source (S6B) raises ValueError, missing params raises ValueError |

## Dependencies

Key libraries (see the `unifier` extra in the root `pyproject.toml`):

- `boto3` — S3 copy operations
- `pyyaml` — source config loading
