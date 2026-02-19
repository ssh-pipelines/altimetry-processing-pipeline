# Crossover Processing

Computes satellite altimetry self-crossovers from daily files. For a given processing date and satellite source, the module finds points where ascending and descending ground tracks intersect within a configurable time window, then interpolates SSH (sea surface height) and time at each crossover location.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## Lambda input

The Lambda receives one item from the jobs manifest per invocation, with `df_version` merged in by the Step Function:

```json
{
  "bucket": "my-bucket",
  "date": "2025-01-01",
  "source": "S6",
  "df_version": "p1"
}
```

| Parameter    | Example      | Description                          |
|--------------|--------------|--------------------------------------|
| `date`       | `2025-01-01` | Processing day (ISO 8601)            |
| `source`     | `S6`         | Satellite source (`GSFC`, `S6`, `S6B`) |
| `df_version` | `p1`         | Daily file generation step (`p1`, `p2`) |
| `bucket`     | `my-bucket`  | S3 bucket for input/output           |

All four fields are required. Available sources are defined in `crossover/config/sources.yaml`.

## Lambda output

```json
{
  "status": "success",
  "data": {
    "bucket": "my-bucket",
    "date": "2025-01-01",
    "source": "S6",
    "df_version": "p1"
  }
}
```

## S3 paths

| Path | Description |
|------|-------------|
| `daily_files/{df_version}/{source}/{year}/{prefix}_{YYYYMMDD}.nc` | Input daily files (read) |
| `crossovers/{df_version}/{source}/{year}/xovers_{source}-{YYYY-MM-DD}.nc` | Output crossover file (write) |

Filename prefix is determined by the global source registry (`utilities/source_registry.py`).

## Directory structure

```
xover/
├── app.py                              # Lambda handler + processor dispatch
├── crossover/
│   ├── parallel_crossovers.py          # Crossover class: data loading, track pairing, output
│   ├── xover_ssh.py                    # Geometric crossover detection (xover_ssh)
│   └── config/
│       ├── sources.yaml                # Per-source orbital parameters
│       └── source_config.py            # SourceConfig dataclass + loader
├── tests/
│   ├── test_crossover.py               # Consistency, empty-input, and all-NaN tests
│   ├── test_source_config.py           # Config loading tests (GSFC, S6, S6B)
│   └── sample_data/
│       ├── sample_inputs/              # 12 daily file granules (gzip-compressed)
│       └── sample_output/              # Reference crossover output (gzip-compressed)
├── Dockerfile
├── requirements.txt
└── README.md
```

## How it works

1. **`stream_files()`** — Globs S3 for daily files within the processing window (day through day + `window_size` + `window_padding`).
2. **`extract_and_set_data()`** — Concatenates all daily files, drops NaN SSH rows, and builds arrays for time, lon, lat, SSH, and track IDs (`cycle * 10000 + pass`). Computes unique tracks, their start times, and a pre-built index for fast per-track lookups.
3. **`search_day_for_crossovers()`** — For each track starting on the processing day, finds candidate crossing tracks (different cycle, opposite pass direction, within one orbital cycle). Calls `xover_ssh()` for each pair.
4. **`xover_ssh()`** — Geometric crossover detection between two ground tracks. Finds where latitude-interpolated tracks cross, interpolates SSH and time at the intersection, and rejects crossovers where the nearest real data point is beyond a distance cutoff (default 30 km).
5. **Output** — Results are filtered to the processing day, sorted by time, and saved as a NetCDF with crossover coordinates, SSH, time, cycle, and pass for both tracks.

## Source configuration

Orbital parameters live in `crossover/config/sources.yaml`:

| Parameter        | Description                                            |
|------------------|--------------------------------------------------------|
| `satellite`      | Satellite name label                                   |
| `crossover_type` | Processing mode (currently always `self`)              |
| `cycle_length`   | Orbital repeat period in days (used as max time diff)  |
| `window_size`    | Days of data to load after the processing day          |
| `window_padding` | Extra days to pad the window                           |
| `max_pass_number`| Maximum pass number for the satellite                  |

Current sources:

| Source | Cycle Length | Window Size | Window Padding |
|--------|-------------|-------------|----------------|
| GSFC   | 9.9156 days | 10 days     | 2 days         |
| S6     | 9.9156 days | 10 days     | 2 days         |
| S6B    | 9.9156 days | 10 days     | 2 days         |

To add a new satellite, add an entry to `crossover/config/sources.yaml`. No code changes required — the stage is source-agnostic.

## Step Function

Defined in `state_machines/xover.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3, merges `df_version` from the parent input into each item, and invokes the `xover` Lambda for each date. Results are written to `pipeline_runs/results/xover/` in S3.

The xover state machine is invoked twice in the along-track pipeline — once with `df_version=p1` (before OER) and once with `df_version=p2` (after OER).

## Development

```bash
cd pipeline/daily_file_gen/xover

# Create venv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run tests (25 total: 13 consistency, 3 empty-input, 3 all-NaN-input, 6 config)
python -m unittest discover -s tests -t . -v
```

### Test data

Sample input/output files in `tests/sample_data/` are gzip-compressed to reduce repo size. The test suite automatically decompresses them to a temp directory at runtime. Input granules contain only the variables used by the processing code (`time`, `latitude`, `longitude`, `ssha_smoothed`, `cycle`, `pass`).

### Consistency tests

`ConsistencyTestCase` processes the 12 sample daily files and compares every output field against a known-good reference file. This ensures refactoring doesn't change results. Tolerances:
- SSH, lon, lat: `1e-10` absolute
- Time: `200ns` (accounts for float64 interpolation rounding)

## Dependencies

Key libraries (see `requirements.txt`):

- `numpy` / `pandas` — numerical computation and data manipulation
- `xarray` / `netCDF4` / `h5netcdf` / `h5py` — reading and writing NetCDFs
- `boto3` / `s3fs` — AWS S3 access
- `dask` — parallel array support (xarray backend)
- `pyyaml` — source config loading
