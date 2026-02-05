# Crossover Processing

Computes satellite altimetry self-crossovers from daily files. For a given processing date and satellite source, the module finds points where ascending and descending ground tracks intersect within a configurable time window, then interpolates SSH (sea surface height) and time at each crossover location.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## Usage

**Lambda event parameters:**

| Parameter    | Example      | Description                          |
|--------------|--------------|--------------------------------------|
| `date`       | `2025-01-01` | Processing day (ISO 8601)            |
| `source`     | `S6`         | Satellite source (`S6`, `GSFC`)      |
| `df_version` | `p1`         | Daily file generation step (`p1`, `p2`) |
| `bucket`     | `my-bucket`  | S3 bucket for input/output           |

**Input:** daily file NetCDFs from `s3://{bucket}/daily_files/{df_version}/{source}/{year}/`

**Output:** crossover NetCDF uploaded to `s3://{bucket}/crossovers/{df_version}/{source}/{year}/`

## Project Structure

```
xover/
  app.py                          # Lambda handler (entry point)
  requirements.txt
  Dockerfile
  crossover/
    parallel_crossovers.py        # Crossover class: data loading, track pairing, output
    xover_ssh.py                  # Geometric crossover detection (xover_ssh)
    config/
      sources.yaml                # Per-source orbital parameters
      source_config.py            # SourceConfig dataclass + loader
  tests/
    test_crossover.py             # Consistency + empty-input tests
    test_source_config.py         # Config loading tests
    sample_data/
      sample_inputs/              # 12 daily file granules (gzip-compressed)
      sample_output/              # Reference crossover output (gzip-compressed)
```

## How It Works

1. **`stream_files()`** — Globs S3 for daily files within the processing window (day through day + `window_size` + `window_padding`).
2. **`extract_and_set_data()`** — Concatenates all daily files, drops NaN SSH rows, and builds arrays for time, lon, lat, SSH, and track IDs (`cycle * 10000 + pass`). Computes unique tracks, their start times, and a pre-built index for fast per-track lookups.
3. **`search_day_for_crossovers()`** — For each track starting on the processing day, finds candidate crossing tracks (different cycle, opposite pass direction, within one orbital cycle). Calls `xover_ssh()` for each pair.
4. **`xover_ssh()`** — Geometric crossover detection between two ground tracks. Finds where latitude-interpolated tracks cross, interpolates SSH and time at the intersection, and rejects crossovers where the nearest real data point is beyond a distance cutoff (default 30 km).
5. **Output** — Results are filtered to the processing day, sorted by time, and saved as a NetCDF with crossover coordinates, SSH, time, cycle, and pass for both tracks.

## Source Configuration

Orbital parameters live in `crossover/config/sources.yaml`:

| Parameter       | Description                                            |
|-----------------|--------------------------------------------------------|
| `cycle_length`  | Orbital repeat period in days (used as max time diff)  |
| `window_size`   | Days of data to load after the processing day          |
| `window_padding`| Extra days to pad the window                           |
| `max_pass_number`| Maximum pass number for the satellite                 |

To add a new satellite, add an entry to `sources.yaml`.

## Development

```bash
cd pipeline/daily_file_gen/xover

# Create venv and install dependencies
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run tests (21 total: 13 consistency, 3 empty-input, 5 config)
python -m unittest discover -s tests -t . -v
```

### Test Data

Sample input/output files in `tests/sample_data/` are gzip-compressed to reduce repo size. The test suite automatically decompresses them to a temp directory at runtime. Input granules contain only the variables used by the processing code (`time`, `latitude`, `longitude`, `ssha_smoothed`, `cycle`, `pass`).

### Consistency Tests

`ConsistencyTestCase` processes the 12 sample daily files and compares every output field against a known-good reference file. This ensures refactoring doesn't change results. Tolerances:
- SSH, lon, lat: `1e-10` absolute
- Time: `200ns` (accounts for float64 interpolation rounding)
