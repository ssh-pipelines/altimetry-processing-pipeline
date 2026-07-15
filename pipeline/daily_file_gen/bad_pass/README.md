# Bad Pass Flagging

Identifies satellite passes with anomalous crossover residuals and writes the flagged results to S3. Runs as an AWS Lambda function, invoked once per date via a Distributed Map in the `bad_pass.asl.json` Step Function.

## How it works

For each processing date, the Lambda:

1. **Gathers crossover files** from a sliding window (10 days back + 1 day pad on each side) under `s3://{bucket}/crossovers/p2/{source}/{year}/`.
2. **Loads crossover data** (cycle, pass, time, SSH) from the netCDF files and computes SSH differences (`ssh1 - ssh2`) for every crossover point.
3. **Flags bad passes** by grouping crossover points by track ID (`cycle * 10000 + pass`) and checking two thresholds:
   - **Mean**: If `n > 15` points and `|mean(dssh)| > 0.1` m
   - **RMS**: If `n > 25` points and `std(dssh) > 0.27` m
4. **Writes results** to `s3://{bucket}/bad_passes/{source}/{date}.json` (only when bad passes are found).

A static list of known bad passes is also maintained in `bad_passes/bad_pass_list.csv`.

## Directory structure

```
bad_pass/
├── app.py                        # Lambda handler
├── bad_passes/
│   ├── bad_pass_flag.py          # XoverProcessor — core logic
│   └── bad_pass_list.csv         # Static list of known bad passes
├── tests/
│   └── test_bad_pass_flag.py     # Unit tests
├── Dockerfile
└── README.md
```

## Lambda input

The Lambda receives one item from the jobs manifest per invocation:

```json
{
  "bucket": "my-bucket",
  "date": "2024-01-15",
  "source": "GSFC"
}
```

All three fields are required.

## Lambda output

```json
{
  "date": "2024-01-15",
  "source": "GSFC",
  "count": 2
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `crossovers/p2/{source}/{year}/xovers_{source}-{date}.nc` | Input crossover files (read) |
| `bad_passes/{source}/{date}.json` | Output bad pass results (write) |

## Step Function

Defined in `state_machines/bad_pass.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `bad_pass` Lambda for each date. Results are written to `pipeline_runs/results/bad_pass/` in S3.

## Running tests

From the repo root (after `uv sync --extra dev`):

```bash
./scripts/test.sh bad_pass
```

## Dependencies

Key libraries (bad_pass has no stage-specific extra; its scientific stack comes from the `pipeline_runtime` extra in the root `pyproject.toml`):

- `netCDF4` / `h5netcdf` / `h5py` — reading crossover netCDF files
- `numpy` — numerical computation
- `boto3` / `s3fs` — AWS S3 access
