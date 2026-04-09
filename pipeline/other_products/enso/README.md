# ENSO

Processes half-degree simple grids into ENSO (El Nino Southern Oscillation) products. Applies diffusion-based smoothing, removes the seasonal cycle and trend, interpolates to a quarter-degree grid, and generates two map visualizations (orthographic and plate-carree projections). Uploads the processed NetCDF and PNG maps to S3.

Runs as an AWS Lambda (see `Dockerfile`), invoked by a Step Function with a JSON event.

## How it works

For each processing date, the Lambda:

1. **Streams the simple grid** from S3 based on the source and date.
2. **Smooths the SSHA field** using an iterative diffusion-based smoother that fills NaN gaps with basin-connected neighbourhood averaging (`smoother.py`).
3. **Removes the seasonal cycle and trend** — interpolates the seasonal SSH reference to the current decimal year, subtracts the seasonal cycle and linear trend from the smoothed data (`ensogridder.py`).
4. **Pads longitudes** to handle wraparound at the 0/360 boundary for clean interpolation.
5. **Interpolates to quarter-degree** (0.25) resolution, then fills small gaps along latitude and longitude.
6. **Masks the output** — applies the quarter-degree basin mask and restricts to ±66 latitude.
7. **Saves the ENSO grid** as a compressed NetCDF with SSHA in mm, then uploads to S3.
8. **Generates two map visualizations**:
   - **Orthographic** (`plot_orth`) — centered on (-150, 10), with satellite name overlay, black background
   - **Plate-carree** (`plot_plate`) — full global view with gridlines, colorbar in mm, titled with satellite name and date
9. **Uploads** the PNG maps to S3.

The satellite name overlay is determined from a hardcoded date-to-satellite mapping (TOPEX/Poseidon through Sentinel-6 Michael Freilich).

## Directory structure

```
enso/
├── app.py                              # Lambda handler (entry point)
├── cartopy_setup.py                    # Pre-downloads Cartopy features for offline Lambda use
├── enso_jobs/
│   ├── __init__.py
│   ├── enso_processing.py              # start_job() orchestration (stream, process, upload)
│   ├── ensogridder.py                  # ENSOGridder class (smooth, detrend, interpolate)
│   ├── ensomapper.py                   # ENSOMapper class (orthographic + plate-carree maps)
│   ├── smoother.py                     # Diffusion-based spatial smoother
│   └── ref_files/
│       ├── akiko_colorscale.txt        # Custom colormap for maps
│       ├── trnd_seas_simple_grid.nc    # Seasonal cycle and trend reference
│       ├── new_basin_mask_quartdeg.nc  # Quarter-degree basin mask
│       └── diff_operator_halfdeg.nc    # Diffusion operator for smoother
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

All three fields are required.

## Lambda output

No explicit return payload on success. On error, raises an exception with a JSON body containing `status`, `errorType`, `errorMessage`, and the original `input`.

## S3 paths

| Path | Description |
|------|-------------|
| `simple_grids/{source}/{year}/{source}_alt_ref_simple_grid_v11_{YYYYMMDD}.nc` | Input simple grid (read) |
| `enso_grids/{source}/ENSO_{YYYYMMDD}.nc` | Output ENSO grid (write) |
| `maps/enso_maps/{source}/ortho/ENSO_ortho_{YYYYMMDD}.png` | Output orthographic map (write) |
| `maps/enso_maps/{source}/plate/ENSO_plate_{YYYYMMDD}.png` | Output plate-carree map (write) |

## Step Function

Defined in `state_machines/enso.asl.json`. Uses a Distributed Map (max concurrency 500) that reads dates from a jobs manifest in S3 and invokes the `enso` Lambda for each date. Results are written to `pipeline_runs/results/enso/` in S3.

Also invoked as part of the `simple_grid_pipeline` orchestration (`state_machines/simple_grid_pipeline.asl.json`), where it runs after the Simple Grids stage and before Indicators.

## Cartopy setup

The Dockerfile runs `cartopy_setup.py` at build time to pre-download coastline, ocean, and land features (110m resolution). At runtime the Lambda operates in offline mode (`CARTOPY_OFFLINE=true`) with no internet calls required for map generation.

## Running tests

From the `enso/` directory:

```bash
python -m unittest discover -s tests -t . -v
```

Tests are minimal — an end-to-end stub that loads sample simple grids and exercises the gridder and mapper.

## Dependencies

Key libraries (see `requirements.txt`):

- `cartopy` / `matplotlib` / `contourpy` — map visualization and projections
- `scipy` — sparse matrix operations for diffusion smoothing
- `numpy` / `xarray` — numerical computation and dataset handling
- `netcdf4` / `h5py` / `h5netcdf` — reading and writing NetCDF files
- `pyproj` — coordinate reference system handling
- `boto3` / `s3fs` — AWS S3 access (via `utilities.aws_utils`)
