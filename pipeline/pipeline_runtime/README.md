# pipeline_runtime

Shared Docker base image for every stage that reads or writes a **daily file**
(NetCDF). Pins the scientific stack — `numpy`, `xarray`, `netCDF4`, `h5py`,
`h5netcdf`, `scipy` — to a single set of versions so producer and consumer
stages agree on encoding behavior across the pipeline.

## Why this exists

The daily-file NetCDF format is the contract between stages (`daily_files`
writes P1; `oer` writes P2; `finalizer` writes P3; `simple_grids` /
`indicators` / `enso` read finalized P3 outputs). The writer and reader
libraries must agree on encoding, attribute serialization, dtype handling,
and time decoding for the artifacts to round-trip correctly.

Before this image existed, each stage pinned its own scientific stack
independently. Drift inevitably crept in (xarray 2023.6 vs 2023.10 vs 2025.4
across stages, netCDF4 1.6.5 vs 1.7.2, …) — an implicit, unowned contract
spread across ten `requirements.txt` files. This image is that contract,
made explicit and singular.

## Who consumes it

| Stage          | Consumes pipeline_runtime |
|----------------|---------------------------|
| `bad_pass`     | yes                       |
| `xover`        | yes                       |
| `daily_files`  | yes                       |
| `oer`          | yes                       |
| `finalizer`    | yes                       |
| `simple_grids` | yes                       |
| `indicators`   | yes                       |
| `enso`         | yes                       |
| `pipeline_init`| **no** — no NetCDF I/O    |
| `unifier`      | **no** — copies P3 files only |

The lightweight Lambdas are deliberately excluded. Pulling them onto the
scientific base would inflate their cold-start images from ~820 MB to over
a gigabyte for no functional benefit.

## How stage Dockerfiles use it

```dockerfile
ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}
```

`BASE_IMAGE` is overridden at build time by `scripts/dev/build_and_push.sh`
and `scripts/prod/build_and_push.sh`, which set it to the matching
`pipeline_runtime` tag in the current environment. All stages built in a
single invocation share the same tag — that's how version coupling is
enforced.

The default falls back to the plain Lambda Python image. That keeps the
Dockerfile buildable on its own (without the wrapper scripts), but a stage
built this way will fail to install its dependencies that expect the
scientific stack — which is the desired failure mode: building without
the wrapper should fail fast rather than silently produce a broken image.

## Bumping a version

1. Edit `requirements.txt` in this directory.
2. Build and push `pipeline_runtime` at the new version (`dev-<sha>` for
   dev, `<semver>` from an annotated git tag for prod).
3. Rebuild every consuming stage at the same version, so each one picks
   up the new base.

The build script verifies (1) and (2) before letting (3) proceed: if any
heavy stage is built without a matching `pipeline_runtime` tag in ECR, the
build aborts.

## Build-only — not a deployable Lambda

`pipeline_runtime` has no `app.py`, no `CMD`, and no corresponding Lambda
function. It exists only as a base image in ECR for stages to `FROM`. The
deploy scripts (`scripts/{dev,prod}/deploy.sh`) recognise it as build-only
and skip the `lambda update-function-code` call. If you add another
build-only artifact in the future, add it to the `BUILD_ONLY_IMAGES` list in
both deploy scripts.

## What's intentionally *not* in this image

- **Per-stage extras** (pandas, dask, geopandas, shapely, pyresample,
  cartopy, matplotlib, pyproj, requests, python-cmr). These are used by
  some stages but not all; promoting them here would bloat every consumer
  for no shared benefit.
- **The `utilities/` package**. Each stage installs that separately, so
  utility updates don't force a base-image rebuild.
- **A CMD or handler**. This image is a base, not a Lambda. Stage images
  set their own `CMD`.
