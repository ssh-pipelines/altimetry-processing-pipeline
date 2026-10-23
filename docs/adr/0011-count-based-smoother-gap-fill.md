# ADR 0011: Smoother gap fill counts omitted samples from local spacing

- **Status**: Accepted
- **Date**: 2026-10-23

## Context

`ssha_smoothing` runs a 19-point filter along track. Sources omit records over
land rather than writing NaN rows, so a window run over the samples alone would
reach across a land crossing and average points hundreds of kilometres apart. The
smoother prevented this by reindexing each day onto an 86,400-point integer-second
grid: every omitted second became a NaN slot.

That grid assumes 1 Hz sampling on whole-second timestamps. The AVISO L2P
high-latitude sources have neither. ERS-1 samples every 0.980468 s, and no
timestamp in any of the four L2P granules inspected lands on a whole second. The
reindex therefore produced an all-NaN grid and the time-keyed lookup back raised
`KeyError`. CryoSat-2, at 91,580 points a day, cannot fit on the grid at all.

The rest of the algorithm (NaN interpolation, mirroring, the all-NaN check) is
position-based. It needs only how many samples are missing between two adjacent
points, not where they would have been.

## Decision

Count the omitted samples instead of placing them on a clock. For each step
between adjacent samples, `round(step / dt_local) - 1` NaN slots are inserted,
where `dt_local` is a 21-step centred rolling median of the surrounding regular
steps (those under 1.5 × `common.sample_period`). The count is capped at 9,
because a slot further than that from a sample cannot fall inside its window.
Windows run over the padded series and are read back at the real samples'
positions.

Filter taps are spaced `ground_speed × sample_period` km apart rather than
`ground_speed × 1 s`.

At 1 Hz on whole seconds this is the grid by construction, and the outputs are
byte-identical: pinned on a full S6 day and on an S6 segment with 1 to 40 samples
deleted.

## Consequences

- Any sampling rate and timestamp alignment works, including sources whose clock
  drifts within a day.
- Where the clock is unstable, a gap's count can be ambiguous: ERS-1's 8.202 s gap
  is 8.366 local periods, and the timestamps do not say whether 7 or 8 samples
  were omitted. It rounds to the nearest whole period count. Counts more than 0.25
  of a period from a whole number are logged at debug level. The effect is
  bounded: outer taps weigh 2e-4 to 1e-2 against a 0.25 peak.
- `common.sample_period` now reaches output through the filter width. A source
  that is not 1 Hz must have it measured; leaving the 1.0 default mis-sizes the
  filter by the rate error (+2% for ERS-1, +6% for CryoSat-2). Gap counts use the
  local median, so they depend on it only through the 1.5 × threshold that
  separates regular steps from gaps.

## Alternatives considered

- **A uniform grid at one configured period.** Predict where every sample should
  land and mark absences. Rejected: misalignment from a uniform grid reaches 48.6%
  of a period for ERS-1 and 39.9% for Jason-1 within one pass. ERS-1 runs two
  interleaved clock rates that drift about 22 periods apart over a day. Only
  CryoSat-2 would have worked.
- **Break the window at gaps.** Discard the far side instead of padding across it.
  Rejected: it narrows the filter at every land crossing on the existing 1 Hz
  sources. Centred on the last point before a 2-sample gap, the window keeps 10 of
  19 slots instead of 16, which would change GSFC, S6, and S3B output.
