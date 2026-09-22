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
| `source`     | `S6`         | Satellite source (`GSFC`, `S6`, `S6B`, `S3B`) |
| `df_version` | `p1`         | Daily file generation step (`p1`, `p2`) |
| `bucket`     | `my-bucket`  | S3 bucket for input/output           |

All four fields are required. Available sources are those with an `xover:` section in `utilities/sources/{source}.yaml`.

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

Filename prefix is determined by the global source registry (`utilities/source_profile.py`).

## Directory structure

```
xover/
├── app.py                              # Lambda handler; picks the spec from crossover_type
├── crossover/
│   ├── processor.py                    # CrossoverProcessor + SelfSpec / ReferenceSpec + SPECS
│   ├── loader.py                       # stream_files, load_track_window
│   ├── search.py                       # find_self_crossovers / find_reference_crossovers
│   ├── results.py                      # pack_records, filter_and_sort, dataset builders
│   ├── track_window.py                 # The loaded-window container
│   ├── xover_ssh.py                    # Geometric crossover detection (xover_ssh)
│   └── config/
│       └── source_config.py            # SourceConfig dataclass + loader
├── tests/
│   ├── test_crossover.py               # Consistency, empty-input, and all-NaN tests
│   ├── test_reference_crossover.py     # Reference-mission crossover tests
│   ├── test_search.py                  # Track-pairing and search tests
│   ├── test_source_config.py           # Config loading tests
│   └── test_app.py                     # Handler + dispatch tests
├── Dockerfile
└── README.md
```

Per-source settings live in `utilities/sources/{source}.yaml`, not in this directory.

## How it works

`CrossoverProcessor` owns the run; the source's `crossover_type` selects a
**`CrossoverSpec`** (`SelfSpec` or `ReferenceSpec` from the `SPECS` registry) that plugs
in the type-specific load / search / to_dataset steps. Everything else is shared:

1. **`spec.load()`** — `stream_files()` globs S3 for daily files in the window, then
   `load_track_window()` concatenates them, drops NaN SSH rows, and builds arrays for
   time, lon, lat, SSH and track IDs (`cycle * 10000 + pass`) plus a per-track index.
   - `SelfSpec` loads **one** window: day through day + `window_size` + `window_padding`.
   - `ReferenceSpec` loads **two**: the high-lat source over `[D-1, D+1]` (neighbor days
     so passes straddling midnight reassemble), and the reference mission over a window
     *centered* on the day, always at `reference_version` regardless of the high-lat
     `df_version` ([ADR 0006](../../../docs/adr/0006-reference-crossovers-against-nasa-ssh-p3.md)).
2. **`spec.search()`** — For each track starting on the processing day, finds candidate
   crossing tracks and calls `xover_ssh()` per pair. `find_self_crossovers()` pairs a
   source against itself (different cycle, opposite pass direction, within one orbital
   cycle); `find_reference_crossovers()` pairs the high-lat window against the reference
   window and interpolates the reference SSH *in time* to the high-lat crossover time.
3. **`xover_ssh()`** — Geometric crossover detection between two ground tracks. Finds
   where latitude-interpolated tracks cross, interpolates SSH and time at the
   intersection, and rejects crossovers where the nearest real data point is beyond a
   distance cutoff (default 30 km).
4. **`pack_records()` / `filter_and_sort()`** — Packs the typed records columnwise, then
   filters to the processing day and sorts by time. `ReferenceSpec` sets
   `two_sided_filter`, so the filter also drops `time1 < day` (its window opens *before*
   the day; the self window does not).
5. **`spec.to_dataset()` → save/upload** — Builds the type-specific `xr.Dataset` and
   writes the NetCDF. The two types carry different schemas, distinguished by a
   `crossover_type` global attribute.

## Source configuration

Orbital parameters live in the `xover:` section of `utilities/sources/{source}.yaml`:

| Parameter        | Description                                            |
|------------------|--------------------------------------------------------|
| `crossover_type` | `self` or `reference` — selects the spec (see above)   |
| `cycle_length`   | Orbital repeat period in days (used as max time diff)  |
| `window_size`    | Days of data to load. Forward-looking for `self`; reinterpreted as a **centered ±** window for `reference` |
| `window_padding` | Extra days to pad the window                           |
| `max_pass_number`| Maximum pass number for the satellite                  |
| `reference_source` | *(`reference` only, required)* The reference mission to cross against — `NASA-SSH` |
| `reference_version` | *(`reference` only, required)* Reference daily-file version to load — always `p3` ([ADR 0006](../../../docs/adr/0006-reference-crossovers-against-nasa-ssh-p3.md)) |

`crossover_type` follows from the source's `product_type`: a `reference` product type
uses `self` crossovers, a `high_latitude` source uses `reference` crossovers. The two
`reference_*` fields are optional on the shared dataclass but validated as required in
`__post_init__` when the type is `reference`.

Current sources:

| Source | Crossover type | Cycle Length | Window Size | Window Padding |
|--------|----------------|-------------|-------------|----------------|
| GSFC   | `self`         | 9.9156 days | 10 days (forward) | 2 days   |
| S6     | `self`         | 9.9156 days | 10 days (forward) | 2 days   |
| S6B    | `self`         | 9.9156 days | 10 days (forward) | 2 days   |
| S3B    | `reference`    | 9.9156 days | 12 days (±)       | 2 days   |

To add a new satellite, add an `xover:` section to its `utilities/sources/{source}.yaml`.
No code changes required for either existing `crossover_type` — the stage is
source-agnostic.

## Step Function

Defined in `state_machines/xover.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3, merges `df_version` from the parent input into each item, and invokes the `xover` Lambda for each date. Results are written to `pipeline_runs/results/xover/` in S3.

The xover state machine is invoked twice in the along-track pipeline — once with `df_version=p1` (before OER) and once with `df_version=p2` (after OER).

## Development

```bash
# From the repo root: create the dev environment (installs every stage's deps
# plus the shared utilities package).
uv sync --extra dev

# Run this stage's tests (25 total: 13 consistency, 3 empty-input, 3 all-NaN-input, 6 config)
./scripts/test.sh xover
```

### Test data

Sample input/output files in `tests/sample_data/` are gzip-compressed to reduce repo size. The test suite automatically decompresses them to a temp directory at runtime. Input granules contain only the variables used by the processing code (`time`, `latitude`, `longitude`, `ssha_smoothed`, `cycle`, `pass`).

### Consistency tests

`ConsistencyTestCase` processes the 12 sample daily files and compares every output field against a known-good reference file. This ensures refactoring doesn't change results. Tolerances:
- SSH, lon, lat: `1e-10` absolute
- Time: `200ns` (accounts for float64 interpolation rounding)

## Dependencies

Key libraries (see the `xover` and `pipeline_runtime` extras in the root `pyproject.toml`):

- `numpy` / `pandas` — numerical computation and data manipulation
- `xarray` / `netCDF4` / `h5netcdf` / `h5py` — reading and writing NetCDFs
- `boto3` / `s3fs` — AWS S3 access
- `dask` — parallel array support (xarray backend)
- `pyyaml` — source config loading
