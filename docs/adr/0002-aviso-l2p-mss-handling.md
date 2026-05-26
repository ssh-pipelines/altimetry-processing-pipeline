# ADR 0002: AVISO L2P MSS handling — bundled DTU21 grid + bilinear interp

- **Status**: Accepted
- **Date**: 2026-05-13

## Context

Reference-mission daily-file processors (S6, GSFC) swap the upstream MSS reference to **DTU21** via the *swap pattern*: each granule ships its source MSS as a per-record variable; a precomputed `<source_mss>_minus_DTU21.nc` diff file ships in the daily_files image; the processor's `_source_mss_correction()` returns `source_mss - dtu21` and the base class adds the result to `ssha`.

For AVISO L2P sources (S3B and the planned successors S3A, SARAL/AltiKa, HY-2B), reproducing the same swap requires knowing which MSS AVISO used to compute the L2P SSHA. That information is poorly documented and may vary across the AVISO L2P product family — a per-collection diff-file family would be needed, with no clean way to validate that we picked the right `source_mss` for each.

## Decision

Skip the swap pattern for AVISO L2P. Bundle a single global DTU21 MSS grid under `pipeline/daily_file_gen/daily_files/daily_files/ref_files/mss/DTU21_mss_global.nc` (LFS-tracked; encoded as `int32` with `scale_factor=0.001` for 1 mm precision, chunked + zlib level 9; lands at **229.5 MB** down from 933 MB raw, bit-exact round-trip).

At processing time, `AvisoL2PDailyFile` bilinearly interpolates DTU21 at each L2P granule's `(lat, lon)` via `scipy.interpolate.RegularGridInterpolator(method="linear")` and treats the result as the AvisoL2P equivalent of S6's `mean_sea_surface_sol2`:

```
ssha_dtu21 = ssha + (l2p["mean_sea_surface"] - dtu21_interpolated)
```

The interpolator is constructed once per Lambda container in a module-level cached singleton (`processing/dtu21.py`); cold start pays the grid load + interpolator build, warm invocations are free. All AVISO L2P sources share the same processor and the same bundled grid.

## Consequences

**Positive:**
- Adding a new AVISO L2P source (SARAL, HY-2B, ...) requires no MSS configuration — just a `utilities/sources/{NAME}.yaml` entry.
- We never need to know AVISO's source MSS — the math cancels regardless.
- Identical `AvisoL2PDailyFile` behavior across all current and future L2P sources.

**Negative:**
- daily_files Lambda image grows by ~230 MB. Acceptable: well under the Lambda 10 GB image limit; ECR pull is cached at the worker level.
- daily_files Lambda memory must accommodate the unpacked grid in RAM. xarray decodes the int32+scale_factor encoding to float64 by default (`mss[10800, 21600]` ≈ 1.87 GB); the dtu21 loader explicitly casts to float32 (~933 MB) to halve resident memory. With working overhead, `MemorySize` ≥ 3072 MB is recommended.
- Pattern divergence from reference-mission processors. Documented here so future readers don't try to "fix" the inconsistency.

## Alternatives considered

- **Mirror the S6 swap pattern with a precomputed `AVISO_minus_DTU21.nc` diff file.** Rejected: requires knowing AVISO's source MSS per collection (poorly documented, may vary across the L2P family); the diff-file family would expand per collection. Bundled-grid + interp gives the same result without that dependency.
- **Stream DTU21 from S3 on demand instead of bundling.** Rejected: `RegularGridInterpolator` construction loads the entire values array into memory, so streaming offers no per-call savings. Cold-start fetch (~2–3 s for 200 MB compressed) buys complexity for marginal benefit.
- **Bundle DTU21 in the `pipeline_runtime` base image** (ADR 0001). Rejected: couples a science-data artifact to a runtime image; bumping DTU21 would force rebuilding every consumer of `pipeline_runtime`. Owning the grid in the daily_files image keeps the version boundary clean.
