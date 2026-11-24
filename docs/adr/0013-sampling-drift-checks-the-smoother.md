# ADR 0013: Onboarding checks sampling drift against the count-based smoother

- **Status**: Accepted
- **Date**: 2026-11-23

## Context

AltiKa's step drifts along a pass (IQR 1.06e-02 s in one granule, against
CryoSat-2's 5.1e-07 s), so before onboarding it science asked whether one
`common.sample_period` is adequate across its full record. Issue #56 specified
a tool for that, `tools/compute_sampling_drift.py`, whose verdict was *needs
per-point treatment* if any pass's samples sit more than 25% of a period off a
uniform grid at the pass's median period.

The issue was written alongside ADR 0011, which had already rejected a uniform
grid for the smoother for exactly that reason: ERS-1 and Jason-1 sit up to half
a period off one. The smoother instead counts omitted samples from each gap's
length in local periods, a 21-step rolling median of the surrounding steps.
Uniform-grid misalignment therefore measures a model the pipeline does not use.
Over ERS-1's record (1992-10-25..1996-06-01), 29,651 of 33,448 passes exceed
25%, worst 47.9%, and ERS-1 smooths correctly. The issue's verdict would fail
every source with interleaved clock rates, whether or not the smoother handles
them.

The smoother depends on the sampling in two ways:

- **Filter width.** Taps are `ground_speed x sample_period` apart, one value per
  source. Where the local period strays from it, the filter is wider or
  narrower by the same fraction.
- **Gap counts.** A gap that is not near a whole number of local periods has an
  omitted-sample count the timestamps do not determine. The smoother rounds it,
  and a wrong count shifts the far side of the window by one slot.

## Decision

The drift tool checks those two dependencies, using the smoother's own
functions (`local_periods`, `ambiguous_gaps`, `sample_offsets`, split out of
`sample_offsets` for this) on whole days as the smoother sees them:

- **Filter width** passes if every pass's 1st-99th percentile local period is
  within 2% of the record's median step. 2% was ERS-1's filter error before
  `sample_period` existed; science judged errors up to CryoSat-2's 6% not
  significant.
- **Gap counts** pass if under 2% of samples, over the record, have an ambiguous
  gap inside their 19-point window, and under 5% in any one cycle.

The limits are about three times the worst ERS-1 values over its record (0.90%;
0.53% and 1.67% in cycle 15). CryoSat-2, over 2010-07-16..2026-07-11 and 165,438
passes, sits at 0.0012% and has no ambiguous gap among 818,638 inside a window. The per-pass median step, IQR and its trend, and
the correlation of step with position along the pass are reported alongside,
as the issue asked. Uniform-grid misalignment stays as a CSV column for
comparison with ADR 0011's figures.

## Consequences

- The verdict answers whether the existing smoother is correct for a source,
  not whether a simpler design would have been. A source that fails goes to
  science; per-point treatment is one possible answer, not the default.
- The limits rest on one accepted source with drift; CryoSat-2's clock is
  constant and does not test them. A source whose cycles sit
  just inside them is worth reading cycle by cycle rather than passing on the
  verdict.
- The tool imports the `daily_files` stage. A change to the smoother's gap
  measurement changes what the tool checks without a second edit.

## Alternatives considered

- **The issue's 25% uniform-grid threshold.** Rejected: ADR 0011 already decided
  the smoother does not use a uniform grid, and ERS-1 fails the threshold while
  smoothing correctly.
- **Reimplementing the gap measurement in the tool.** Rejected: the check would
  drift from the code it claims to check.
