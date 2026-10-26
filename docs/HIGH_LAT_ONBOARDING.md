# Onboarding a high-latitude source

Bringing up a new `high_latitude` **Source** — one AVISO L2P satellite publishing its
own `NASA-SSH_{source}` along-track line. No new code is needed for a source whose
wire format is L2P: `AvisoL2PIngestor` and `AvisoL2PDailyFile` are named for the
format, not for S3B, and every stage dispatches off `product_type`.

What *is* needed is a set of measured per-source constants, and they can only be
measured from the pipeline's own output. That is the shape of this procedure: passes
through the along-track chain, with a calibration step and a redeploy between them.

Read [ADR 0006](adr/0006-reference-crossovers-against-nasa-ssh-p3.md) first — the
reference-mission ordering constraint governs the whole sequence.

## The bootstrap shape, and why

A `high_latitude` source has no self-crossovers. Its SSHA is tied to the reference
mission at **reference-mission crossovers**, which means four of its config values
cannot be known until it has already produced files:

| Value | Measured from | Needs | Consumed by |
|---|---|---|---|
| `common.sample_period` | P1 timestamps | ~2 weeks of P1 | daily-file smoothing |
| `common.ground_speed` | P1 geometry | ~2 weeks of P1 | `oerfit` knot placement, daily-file smoothing |
| `common.intermission_bias` | p1 crossovers | the full record | reference OER de-biasing, finalizer datum tie |
| `finalizer.pass_flag` thresholds | p2 crossover statistics | the full record | bad-pass flagging |

Each constant is deployed Lambda config, so measuring one requires a **redeploy**
before any stage can use it. That gives three passes, and the ordering matters
because the constants are not independent — `ground_speed` and `sample_period` set
the smoothing filter, so they have to be right *before* the P1 that the bias is
measured from:

| Pass | Window | Runs | Produces |
|---|---|---|---|
| 1 | ~2 weeks | `daily_files` | `ground_speed`, `sample_period` → redeploy |
| 2 | full record | `daily_files` → `xover(p1)` | `intermission_bias` → redeploy |
| 3 | full record | the whole chain, `force_update: true` | P3 + published files |

Only pass 2's P1 is expensive, and it is never thrown away. Pass 1's output is
throwaway by design: it exists to measure geometry and timing, which neither a wrong
`ground_speed` nor a wrong `sample_period` affects — the daily files carry the
upstream coordinates and timestamps unchanged.

The redeploys are the steps most easily skipped, and skipping one is quiet — a
missing bias redeploy produces a plausible-looking P3 with no bias applied.

### Why pass 1 is separate

A placeholder `ground_speed` does not fail loudly. At `0` the smoothing filter
degenerates from a Gaussian to a flat 19-point boxcar:

```
ground_speed = 0       → weights all 0.0526                    (boxcar)
ground_speed = 6.6943  → weights 0.0 … 0.2518 … 0.0  (Gaussian, ~7 points wide)
```

So `ssha_smoothed` is *wrong* rather than absent — and `ssha_smoothed` is exactly what
`xover` reads. A bias measured from crossovers built on boxcar-smoothed P1 inherits the
error silently. Hence: short window, measure geometry, redeploy, then the real P1.

If a reliable `ground_speed` and `sample_period` are already available for the mission
from another source, pass 1 can be skipped — it exists only to measure those two
numbers.

## Prerequisite: the reference line must be published for the window

Reference crossovers read `products/{reference_product}/along_track/{year}/` and
**missing days are logged and skipped, not fatal** — an absent reference backfill
yields a valid, nearly-empty crossover file, and `compute_intermission_bias` then
reduces a handful of points to a confident-looking number.

Pass 1 does not read the reference line, but pass 2 does, so confirm this no later
than the pass 1 redeploy: the reference along-track product must exist across the new
source's full date range. For a 1990s mission this means the reference backfill must
reach back that far; `products/` is recent, so do not assume it. Verify by listing
every year of the window and counting files against the days in it, not by inference.

## Step 1 — Write the source YAML

Copy the nearest existing high-latitude profile and then go through it field by
field. Most of what needs changing is not obviously wrong on inspection, because a
copied value is a *valid* value.

```yaml
common:
  product_type: high_latitude   # drives processor, product identity, every dispatch
  discovery_type: thredds
  enabled: true                 # see "On `enabled`" below
  start_date: "1992-10-25"
  end_date: "1996-06-01"
  ground_speed: 0               # PLACEHOLDER — measured in step 3
  intermission_bias: 0          # PLACEHOLDER — measured in step 5
  sample_period: 1.0            # PLACEHOLDER — measured in step 3
  collections:
    - shortname: dataset-l2p-uncross-calibrated-ntc-sla-e1
      thredds_collection: dataset-l2p-uncross-calibrated-ntc-sla-e1
      thredds_version: version_04_00
      priority: 1
      source_label: "..."
      source_url: "..."
      reference: "https://www.aviso.altimetry.fr/"
```

Mark placeholders as placeholders. A copied comment asserting a median over three
million crossovers, sitting above a value of `0`, is worse than no comment — it
states a measurement that was never made for this source. The ERS-1 profile carried
exactly that until its ground speed was measured.

Fields that must be re-derived rather than inherited:

- **`start_date` / `end_date`** — from the THREDDS catalog's actual cycle coverage,
  not from the mission's nominal lifetime. Crawl the collection catalog and read the
  first and last cycle's filename timestamps.
- **`sample_period`** — instrument-dependent. See *Sampling rate* below and step 3.
- **`ground_speed`** — altitude-dependent. See step 3.
- **`intermission_bias`** — see step 5.
- **`finalizer.pass_flag` thresholds** — noise-dependent. Older single-frequency
  missions are substantially noisier than S3B, so inherited thresholds will
  over-flag. Inherit them to begin with, then recalibrate against the p2 crossover
  statistics (step 7).
- **`xover.cycle_length`** — this is the **reference** mission's repeat period, not
  the new source's. On the reference path it is only used as the origin-split gap
  threshold in `find_reference_crossovers`. Inheriting `9.9156` is correct; say so in
  a comment, because it reads like a bug otherwise.

### Check the orbit-phase structure before trusting one date range

Missions with non-uniform orbit phases do not fit the one-`start_date`-to-`end_date`
model as cleanly as a repeat-orbit mission does. Crawl the collection catalog and
compare per-cycle file counts and date spans before settling the window:

```bash
curl -s "https://tds-odatis.aviso.altimetry.fr/thredds/catalog/<collection>/<version>/catalog.xml" \
  | grep -o 'cycle_[0-9]*' | sort -u
```

Then for a few cycles across the record, check the file count, the highest pass
number, and the date span. Short cycles, pass numbers in the thousands, and missing
cycle directories all indicate distinct orbit phases. Decide explicitly which phases
are in scope and record the decision in the YAML — the date window alone does not
express it.

### Sampling rate: measure it

Do not assume 1 Hz. The `reference` sources are 1 Hz on whole-second timestamps; none
of the AVISO L2P granules inspected so far is (ERS-1 0.980468 s, CryoSat-2
0.943437 s, Jason-1 1.007738 s, AltiKa 1.040686 s — see
[SAMPLING_RATE_FINDINGS.md](SAMPLING_RATE_FINDINGS.md)). A collection name sometimes
states the rate (`...-s3b-1hz`); most do not.

`sample_period` sets the smoothing filter's tap spacing (`ground_speed ×
sample_period` km). Leaving the `1.0` default does not fail; it mis-sizes the filter
by the rate error — +2% for ERS-1, +6% for CryoSat-2. Gap counting uses a local
rolling median of the steps, so it tolerates a drifting clock and depends on
`sample_period` only through the 1.5× threshold separating regular steps from gaps
([ADR 0011](adr/0011-count-based-smoother-gap-fill.md)).

`compute_ground_speed` measures it from the pass 1 files alongside `ground_speed`
(step 3), and warns when the configured value differs. Leave the `1.0` placeholder
until then; pass 1's output is discarded either way.

A single constant is adequate only where the rate is stable within a pass. CryoSat-2
and Jason-1 are constant to numerical noise; ERS-1 runs two interleaved rates
2.4e-4 s apart, which the local median absorbs. AltiKa's step drifts along the pass
(IQR 1.06e-02 s) and needs the full-record drift analysis before it is onboarded. The
tool's range of per-pass medians shows drift *between* passes; it does not detect
drift within one.

### On `enabled`

`enabled: true` is required even for a manual dev run: `start_run.resolve_contributors`
refuses to fan out over a line with zero *enabled* contributors. It is the declared
production fact, so setting it has a paired obligation (step 2). A source onboarded
ahead of production goes back to `false` when the passes are done (step 8).

## Step 2 — Wire the two registries

Both are pinned by tests, so getting this wrong fails the suite rather than the
deploy. Run `./scripts/test.sh daily_files utilities` to confirm.

**`SOURCE_REGISTRY`** in `daily_files/daily_file_job.py` — which components handle the
source. For an L2P source this is the same four lines as every other L2P source
(`HttpDownloader` + `build_aviso_session`, `AvisoL2PIngestor`, `AvisoL2PDailyFile`).
Without it, `start_job` raises `SourceNotSupported` on the first invocation.

**`utilities/schedules.yaml`** — a reviewable mirror of what exists in EventBridge.
Nothing in the repo deploys it. A new source needs an entry keyed on its published
identity (`NASA-SSH_{source}`), because `enabled: true` on the Source without one
means the source looks configured and never runs. Until the rule actually exists in
AWS, stage it:

```yaml
  nasa-ssh-<source>:
    cron: "TBD"          # marks a rule that does not exist in AWS yet
    enabled: false       # mirrors the rule's own state, independent of the Source's
    input:
      published_product: NASA-SSH_<SOURCE>
```

## Triggering a run

Every pass below starts the same way: deploy, then start an execution scoped to the
new line.

```bash
scripts/dev/pipeline.sh
```

The trigger is product-keyed —
`{"published_product": "NASA-SSH_<SOURCE>", "start": ..., "end": ..., "bucket": ...}`.
A high-latitude line has no **product tier**, so the gridded branch is unreachable
from this trigger rather than merely skipped: ENSO and indicators cannot fire.

Two `pipeline_init` planning behaviours govern what a re-run actually does:

- Dates **after** the last upstream granule are deliberately not planned, and a window
  with nothing enumerated plans nothing at all. Backfilling a gap needs a range that
  extends past it, so real granules bound it.
- Re-running does **not** regenerate by default: `plan_jobs` skips a date whose
  existing **P3** is newer than its upstream granules. Every pass after the first
  changes *config*, not upstream data, so each needs `force_update: true`.

### Running part of the chain

Passes 1 and 2 stop short of the full chain, and the product trigger always runs it to
completion. To run only some stages, drive the per-stage state machines directly —
they are exactly the machines `at_pipeline` chains, and each one ends after its own
Distributed Map. The chaining lives in `at_pipeline`, not in the stage machines, so
invoking one runs that stage and stops.

**Two steps. First, plan a window** by invoking `pipeline_init` once:

```json
{
  "bucket": "<bucket>",
  "source": "<SOURCE>",
  "run_id": "20261022T120000",
  "start": "1992-10-25",
  "end": "1992-11-08",
  "force_update": true
}
```

`run_id` is required and deliberately not defaulted — `start_run` normally mints it so
both tiers share one identity. Any string works for a manual run; use a timestamp so
the run paths stay sortable. The response carries the `jobs_key` the next step needs.

**Then start the stage machine**, `nasa-ssh-pipeline-<stage>-<STAGE_ENV>-sm`:

| Stage machine | Input beyond `bucket` + `jobs_key` |
|---|---|
| `daily_file` | — |
| `xover` | `df_version` (`p1` or `p2`) |
| `oer` | `df_version` |
| `bad_pass` | — |
| `finalizer` | — |
| `publisher` | `family`, `published_product` |

```json
{
  "jobs_key": "pipeline_runs/<SOURCE>/<run_id>/jobs.json",
  "bucket": "<bucket>",
  "df_version": "p1"
}
```

Note what is *not* in there: `source` and `date` come from each manifest item, since
the Map reads the manifest `pipeline_init` wrote. That is also why one manifest can
drive several stages in sequence — plan the window once, then run `daily_file`, then
`xover` with the same `jobs_key`.

Results land under `pipeline_runs/<SOURCE>/<run_id>/results/<stage>/`, the same place
a full run writes them, so `run_summary` and the diagnostic tools see no difference.

Caveats:

- `daily_file-sm` opens with a `Podaac creds update` task. AVISO sources never touch
  PODAAC credentials, so a failure there is a failure on a step the source does not
  need — the function is a prod-deployed singleton (`deploy_stages: [prod]`).
- Invoking the stage **Lambdas** one date at a time is the harder path, not the easier
  one: `daily_files` requires a `granules` list of resolved download URLs, which means
  running the upstream enumeration by hand. `pipeline_init` exists to do that. If a
  stage machine is blocked, read the `granules` arrays out of its manifest rather than
  rebuilding them.

## Step 3 — Pass 1: measure ground speed and sample period

Run `daily_files` only, over a short window — **~2 weeks is plenty**, and the window
need not be at the start of the record. Use the `daily_file` stage machine (see
*Running part of the chain*); the product trigger would run the whole chain.

### Check the pass 1 files

These files are throwaway, but they are the first output the source has produced, and
catching a problem here is cheaper than catching it after the full-record pass 2.
Open a few and check:

- `ssha_smoothed` is populated — most points finite, not all-NaN
- `latitude` extends past the reference mission's inclination limit
- the `ssha` median over the window is near zero. A level offset of tens of
  centimetres means the MSS swap went wrong: S3B's ~0.68 m offset was the bundled
  DTU21 grid sitting on the T/P ellipsoid rather than WGS84

ERS-1, 15 files from 1992-10-25 to 1992-11-08: ~48,000 points/day, 91–92% of points
with finite `ssha_smoothed`, latitude −78.5 to 81.5, `ssha` median 0.018 m.

### How many files the measurement needs

`compute_ground_speed` groups points by `(cycle, pass)`. For ground speed it keeps
passes with ≥`_MIN_PASS_POINTS` (1000) *and* ≥`_COMPLETENESS_FRACTION` (0.9) of the
longest pass observed in the loaded set; each qualifying pass yields a median speed,
and the reported value is the median across passes. For sample period every pass
counts, since a truncated pass still has regular steps; each yields its median
non-gap step, and the reported value is again the median across passes.

**Expect most passes to be rejected.** Land and ice crossings truncate passes, so only
the passes with the fewest gaps reach 90% of the longest. For ERS-1, 15 files held 425
passes and 57 qualified. That was enough: the qualifying passes span 6.7222–6.7287 km/s
(5th–95th percentile), well inside the precision knot placement needs. Judge the
window by that spread rather than by a pass count, and widen it if the spread is
loose.

**Do not use a single file.** Passes are grouped across all loaded files concatenated,
so a pass crossing midnight is only reassembled when both its days are present. With
one file every pass at both ends is truncated, which is precisely what the
completeness filter discards. Two or more files and the interior passes are whole.

### Running the tool

From S3:

```bash
python tools/compute_ground_speed.py --source <SOURCE> --bucket <bucket> --version p1
```

Or against P1 files copied to a local directory (searched recursively, filtered to
this source's filenames):

```bash
uv run python tools/compute_ground_speed.py --source <SOURCE> --daily-dir <dir>
```

`--version` defaults to `p2`, which does not exist yet — pass `p1` explicitly in S3
mode. Geometry is identical across lifecycle versions, so that is the only reason it
matters. `--max-passes` caps **files**, not passes, and the file list is sorted, so it
takes the chronologically earliest.

ERS-1:

```
  Ground speed (ERS1): 6.7267 km/s
  Passes used: 57  (point counts 2317–2570)
  Sample period (ERS1): 0.980468 s
  Passes used: 425  (per-pass medians 0.980468–0.980469)
```

Ground speed is distance over elapsed time, so it is correct at any sampling rate. A
`WARNING` line after the results means the configured `sample_period` differs from the
measurement — expected on pass 1 with the placeholder, a problem on any later pass.

The tool reports only the median ground speed, not its spread across passes. To check
the spread, or whether relaxing the completeness filter moves the median, call
`pass_ground_speed` per `(cycle, pass)` from Python. For ERS-1, keeping 393 passes of
≥1000 points instead of 57 moved the median to 6.7283 (+0.02%).

Write both results under `common` with the pass counts and the window they were
measured over:

```yaml
  ground_speed: 6.7267     # km/s, median ground speed over 57 long passes, 1992-10-25..1992-11-08 P1 (tools/compute_ground_speed.py)
  sample_period: 0.980468  # s, median sample step over 425 passes, same window
```

Then run `./scripts/test.sh daily_files utilities`, and **redeploy** so the Lambdas
pick up both `ground_speed` and `sample_period`. Discard this pass's P1 files; they
are boxcar-smoothed (see *Why pass 1 is separate*).

## Step 4 — Pass 2: the real P1, and first crossovers

Confirm the reference line covers the full record first (see *Prerequisite*). Then,
with `ground_speed` and `sample_period` deployed, run `daily_files` → `xover(p1)` over
the full record, `force_update: true`. One `pipeline_init` call plans the window; the
`daily_file` machine then the `xover` machine (`df_version: p1`) run against that one
`jobs_key`.

Open one P1 file and repeat the pass 1 checks, plus:

- cycle and pass numbers match the granule filenames

Then one p1 crossover file: `ssh1` (high-lat) and `ssh2` (time-interpolated reference)
should both be populated. All-NaN `ssh2` against valid `ssh1` means no reference bracket
could be formed — either the reference window has gaps, or `window_size` is too narrow
to capture a before/after pair at this latitude. This is the step that depends on the
published reference line, so if crossovers come back empty or thin, re-check the
prerequisite before suspecting the new source.

## Step 5 — Measure the intermission bias

```bash
python tools/compute_intermission_bias.py --source <SOURCE> --bucket <bucket> \
  --version p1 --plot
```

`--xover-dir <dir>` reads local crossover files instead of S3, and `--start-date` /
`--end-date` restrict the baseline window.

The printed **median** is the value to use. `--plot` writes three diagnostics to
`tools/diagnostics/<SOURCE>/` that probe orthogonal ways the single-constant assumption
breaks: temporal drift, distribution shape, and latitude structure. If the low-pass
curve slopes, or the latitude bins trend, a constant is not adequate and the science
side needs to weigh in before this source goes further. The tool reports a Theil-Sen
trend as context and deliberately does not label it significant — judge from the plots.
The low-pass cutoff default was validated for S3B; `--lowpass-cutoff-days` changes it.

Check the crossover count against what the record length warrants. Orders of magnitude
low means step 4's crossovers were thin, not that the bias is well-constrained.

For a mission with several orbit phases, also run the tool once per phase with
`--start-date` / `--end-date`. A level step between phases shows on the low-pass plot as
a plateau, not a slope, and the Theil-Sen trend misreads one that falls mid-record as
drift. ERS-1 stepped by about 11 mm (0.0209 m in phases C and G, 0.0102 m in the
geodetic phase), and AVISO's own `inter_mission_bias` changes at the same boundaries.

A step of that size does not need a per-phase constant. The reference OER fits each
day's own crossovers, so it removes whatever offset that day carries: on ERS-1, final
`ssha` was the same at a median of 0.00 mm whether `intermission_bias` was 0 or
0.0175. The constant matters only where the spline is held at zero: at the ends of
the day's data, across gaps of 0.75 h or more (0–5% of points on the days tested),
and on days with no crossovers at all.

Write the result to `common.intermission_bias` with the count and the comparison it was
measured against, replacing the placeholder comment, then **redeploy**.

## Step 6 — Pass 3: the full chain

Re-run the whole along-track chain with `force_update: true` — this is the pass to use
the product trigger for, since every stage runs. The bias affects the finalizer rather
than P1, but `force_update` replans from the start, so the chain runs end to end:

```
daily_files → xover(p1) → oer → xover(p2) → bad_pass → finalizer → publisher
```

Two things about the reference OER path, both intentional and both surprising if you
expect the self path:

- The `ssh_max_error = 0.3 m` gate is **disabled** on the reference path
  (`apply_error_gate = False`), so a large intermission bias does not empty the fit.
- OER de-biases `dssh` by `intermission_bias` so the spline captures pure orbit
  error; the finalizer then applies the same constant to the absolute `ssha` level.
  Two stages, one constant, different purposes. The finalizer records
  `intermission_bias_applied` and removes a previously-applied value before applying
  a new one, so re-running after changing the bias is safe and idempotent.

## Step 7 — Calibrate the pass-flag thresholds

With p2 crossovers in hand, compare the per-pass mean and RMS statistics against the
inherited `mean_threshold` / `rms_threshold`. A noisier mission flags a large
fraction of its passes under thresholds tuned for a quieter one. Record what the
flagged fraction actually is; a threshold change needs that number as its
justification.

Group the crossovers by `(cycle1, pass1)` and compute each pass's mean and standard
deviation of `ssh1 - ssh2`, counting only passes with more crossovers than
`mean_num` / `rms_num`. ERS-1 inherited S3B's thresholds (0.1 m mean, 0.27 m RMS)
unchanged. Over 25,386 passes, the per-pass |mean| had a 99th percentile of 0.070 m
and the RMS a maximum of 0.154 m, even before OER. The full-record run flagged three
passes on one date, 1995-11-15. AVISO flags 9 of that day's passes entirely, so the
spline is held at zero beside the gaps they leave, and those three passes' offsets
survive OER.

## Step 8 — Documentation

Per-source tables that go stale, all of which list every source explicitly:

- `docs/DATA_FLOW.md` — the diagram node's `TBD → TBD` becomes real dates; drop the
  source from the `disabled` class line once it is enabled in production
- `pipeline/daily_file_gen/daily_files/README.md` — collection table
- `pipeline/daily_file_gen/pipeline_init/README.md` — discovery table
- `pipeline/daily_file_gen/xover/README.md` — crossover config table
- `pipeline/daily_file_gen/finalizer/README.md` — offset / bias / date-range table
- `CHANGELOG.md` under `[Unreleased]`

If the source is not going to production yet, set `common.enabled` back to `false` and
leave its `schedules.yaml` entry staged. `test_schedules` passes either way, since a
disabled source needs no schedule, and a staged rule is disabled too.

An L2P source introduces no new vocabulary, so `docs/GLOSSARY.md` needs nothing. It
also makes no new architectural decision, so no ADR — unless the orbit-phase question
from step 1 forces one, which it would if a mission needs more than a single date
range or a single `cycle_length`.

## Checklist

```
Config
[ ] Source YAML: dates from the catalog, placeholders marked as placeholders
[ ] Orbit phases inspected; in-scope phases decided and recorded
[ ] cycle_length commented as the REFERENCE mission's repeat, not this source's
[ ] SOURCE_REGISTRY entry added
[ ] schedules.yaml entry added (cron: TBD, enabled: false until the rule exists)
[ ] ./scripts/test.sh daily_files utilities  — green
[ ] uv run ruff check .                      — green

Pass 1 — ground speed + sample period (~2 weeks)
[ ] Deploy; pipeline_init a 2-week window, then the daily_file stage machine
[ ] P1 files: ssha_smoothed populated, latitude past the reference limit,
    ssha median near zero
[ ] compute_ground_speed --version p1 (or --daily-dir), never 1 file
[ ] Spread across qualifying passes tight enough; widen the window if not
[ ] Per-pass sample-period medians tight (no drift between passes)
[ ] ground_speed and sample_period written with pass counts and window
[ ] Reference along-track product published across the full date range — verified by listing
[ ] REDEPLOY

Pass 2 — real P1 + crossovers (full record, force_update: true)
[ ] pipeline_init the full record, then daily_file then xover (df_version: p1)
[ ] One P1 file: pass 1 checks, cycle/pass match granules
[ ] One p1 crossover file: ssh2 populated
[ ] compute_intermission_bias --version p1 --plot
[ ] Diagnostic plots reviewed for drift and latitude structure
[ ] Multi-phase mission: median compared per orbit phase
[ ] Crossover count plausible for the record length
[ ] intermission_bias written with its count, placeholder comment replaced
[ ] REDEPLOY

Pass 3 — full chain (full record, force_update: true)
[ ] Product trigger: daily_files -> xover(p1) -> oer -> xover(p2) -> bad_pass -> finalizer -> publisher
[ ] pass_flag thresholds checked against the actual flagged fraction

Docs
[ ] 4 README tables + DATA_FLOW node + CHANGELOG
[ ] Not going to production yet: enabled back to false, schedule left staged
```
