# ADR 0012: Orbit swap ported from C to numpy

- **Status**: Accepted
- **Date**: 2026-11-16

## Context

S6 ingest recomputes SSHA against a JPL precise orbit. Until now this ran
`interpPosGoaToNetCDFtimes.e`, a C program from JPL (S. Desai), as a subprocess.
It was compiled in a `c_builder` stage against netCDF-C, and its shared libraries
were copied into the image and put on `LD_LIBRARY_PATH` for the whole Lambda
process. On the AL2 base image netCDF came from EPEL.

The move to `lambda/python:3.12` (AL2023) removed EPEL. Two ways to keep the C
were built and checked against the AL2 binary on real S6 granules, and both
produced byte-identical output:

- netCDF-C and HDF5 compiled from source in `c_builder`. This adds a slow build
  under amd64 emulation and versions to track by hand.
- `netcdf-devel` from SPAL, Amazon's rebuild of EPEL9. SPAL is outside AWS
  Enterprise Support, is patched only when upstream EPEL9 is, and has no AWS CVE
  tracking. Its netCDF also links HDF4, libjpeg, and libtirpc, none of which the
  program uses, so the bundled set grew to eight libraries.

Either way the bundled libraries reach the runtime image as loose `.so` files
with no package record, so image scanning does not report on them, and
`LD_LIBRARY_PATH` lets them shadow the copies netCDF4 and h5py ship for Python.

The program itself is small: it reads `time_tai`, `altitude`, and `ku/ssha` from
the granule, interpolates the orbit with an 8-point Lagrange stencil, converts
ECEF to geodetic height on the product's ellipsoid by Newton iteration, and adds
the height difference to SSHA. The Python ingest already holds the granule bytes
and parsed the program's text output back into an array.

## Decision

Port the program to numpy (`daily_files/ingestion/orbit_swap.py`) and delete the
`c_builder` stage, the bundled libraries, and `LD_LIBRARY_PATH`. The port keeps
the C's operation order (Lagrange weights by sequential division then
multiplication, per-sample convergence of the Newton iteration) so the remaining
difference is libm versus numpy rounding.

Against the C printing at full precision, over 67 S6A NTC passes from 2021 to
2026 (224,795 samples), the port differs by 0.76 nm RMS and at most 4.4 nm, with
the same samples kept and the same fill pattern. Against the C as it ran in
production, which printed SSHA to 1 µm, the difference is at most 0.5 µm. The
orbit correction itself is of order millimetres. Run on ku/ssha as the C was, a
real S6 2026-09-05 daily file produced with the port matches the production file
on every variable except `ssha` and `ssha_smoothed`, which differ by at most
0.5 µm.

The C read `ku/ssha` (ocean retracker) and its result was stored as `ssha_nr`
(numerical retracker), so swapped passes carried the wrong retracker's SSHA and
fill pattern. The correction depends only on time, altitude, and the orbit, not on
SSHA, so the port applies it to `ku/ssha_nr` instead; the science team approved
this on 2026-11-16. On the 2026-09-05 file this changes `ssha` on samples flagged
good by a median of 0.8 mm (5–95%: −2.7 to +3.9 mm), with 27 of 47,599 beyond
5 cm. 1,811 samples gain a valid `ssha` that the ocean retracker's fill pattern
had dropped, 91 lose one, and `nasa_flag` changes on 140.

Any failure of the swap now fails the job: an orbit file that cannot be
downloaded, one that does not span the pass, a length mismatch, or an exception
in the port. Previously each of these fell back to the unswapped `ssha_nr` for
that pass with only a warning, so a daily file could silently mix orbits. The
cost is that a date fails until JPL publishes its POE or MOE.

## Consequences

- `daily_files` builds from `pipeline_runtime` with no compiler or system
  packages, and no third-party repository is enabled at build time.
- The swap is covered by the stage's unit tests against a trimmed real granule
  and the C's full-precision orbit correction for it
  (`tests/sample_data/S6A_orbit_swap_subset*`), instead of being mocked.
- No subprocess, temporary granule copy in `/tmp`, or 120 s timeout.
- The C source stays under `ingestion/interpPosGoaToNetCDF/` as the reference
  the port was checked against; it is no longer built.
