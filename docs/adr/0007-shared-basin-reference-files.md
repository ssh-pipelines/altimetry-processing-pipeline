# ADR 0007: Shared reference-file directory for duplicated basin assets

- **Status**: Proposed (header-tolerant loader + connection-table header: **implemented**; directory move: **pending**)
- **Date**: 2026-08-24

## Context

Several pipeline stages ship the *same* basin-definition reference files, copied
independently into each stage's `ref_files/` directory. Two assets are currently
byte-identical duplicates across stages (verified by md5):

| Asset | Copies in | Size each |
|---|---|---|
| `basin/new_basin_lake_polygons.{shp,shx,dbf,prj}` | `simple_grids`, `daily_files` | ~1.0 MB |
| `new_basin_mask_quartdeg.nc` | `simple_grids`, `enso` | ~2.0 MB |

Each consumer reaches for its own local copy with an independent path convention:

- `simple_grids` — hardcoded relative strings in `simple_gridder/gridding.py`
  (`simple_gridder/ref_files/basin/new_basin_lake_polygons.shp`,
  `simple_gridder/ref_files/new_basin_mask_quartdeg.nc`).
- `daily_files` — a `REF_FILES_DIR` constant in `daily_files/config/paths.py`,
  joined in `processing/empty_template.py` and `processing/daily_file.py`.
- `enso` — hardcoded relative string in `enso_jobs/ensogridder.py`.

The **drift risk** is concrete and the whole reason this surfaced: regenerating the
basin definition (exactly the kind of edit just made to `basin_connection_table_v2.txt`)
means hand-updating every copy. Miss one and the copies silently diverge. Because
basin IDs drive both the gridder's adjacency logic *and* the daily file's
`basin_names_table`, a divergence is a correctness bug that no test would catch
today.

This is an **implicit, unowned contract** in the same spirit as the target-registry
drift ADR 0004 addressed: "the canonical basin definition" has no single home, so
consistency is maintained by hand and hope.

### Why this is safe to consolidate

The Docker build context is always the repo root
(`docker buildx build -f "$DIR/Dockerfile" "$REPO_ROOT"` in
`scripts/util/_build_and_push.sh`). Any stage's Dockerfile can therefore `COPY`
from anywhere in the repo — the duplication is **not** forced by build isolation.
There is already an established precedent for a single shared source consumed by
every stage: the top-level `utilities/` package, which each Dockerfile pulls in via
`COPY utilities/ utilities/`.

### What is *not* a candidate

The remaining `ref_files/` assets are each single-consumer and stay where they are —
moving them adds indirection with no dedup benefit:

- `daily_files` only: the large MSS grids (`DTU21MSS` ~281 MB, the two `mss_diffs`
  ~270 MB each), `complete_gsfc_pass_lut.csv`, `rads_bias.yaml`.
- `simple_grids` only: `new_basin_mask_halfdeg.nc` (referenced only by
  `gridding.py`), `basin_connection_table_v2.txt`.
- `indicators` only: `ann_pattern.nc`, the `*_pattern_and_index.nc` set,
  `half_deg_grid_cell_areas.nc`, `txt_templates/`.
- `enso` only: `trnd_seas_simple_grid.nc`, `diff_operator_halfdeg.nc`,
  `akiko_colorscale.txt`.

## Decision

Introduce a **single shared reference-file directory** at the repo root as the
canonical home for cross-stage basin-definition assets, and have each consuming
Dockerfile `COPY` from it into the location its code already expects.

Move these assets there:

- `new_basin_lake_polygons.{shp,shx,dbf,prj}` (duplicated: `simple_grids`, `daily_files`)
- `new_basin_mask_quartdeg.nc` (duplicated: `simple_grids`, `enso`)
- `basin_connection_table_v2.txt` (single-consumer today, but a basin definition — see
  "Connection table" below for why it joins the move)

For the two duplicated assets, loaders remain unchanged — the shared files land at
each stage's existing runtime path via `COPY`, so no in-code path edits are required.
Only the three Dockerfiles (`simple_grids`, `daily_files`, `enso`) gain a
`COPY <shared>/… …` line, and the duplicate copies are deleted from the stage trees.
The connection table's move *does* update one path string in
`simple_gridder/gridding.py` (its loader is the only reader).

### Placement and naming

Create a **top-level `reference_data/` directory** (sibling of `src/`,
`state_machines/`), with the basin assets under `reference_data/basin/`. The basin
mask and the basin shapefile live together there — they are the same basin
definition in raster and vector form.

This placement is chosen against the planned reorganization, which renames 
`pipeline/` → `src/` and moves `utilities/` → `src/shared/`:

- **Not `src/shared/`.** That target is a `pip install`-ed Python package baked into
  every Lambda image; the reorg is actively *narrowing* what lives there (it wants to
  evict even the build tooling as "the wrong dependency direction — build tooling
  shipped inside the runtime artifact"). A few MB of shapefiles and NetCDF grids are
  not Python and do not belong in site-packages.
- **Top-level, like `state_machines/`.** The reorg keeps `state_machines/` at repo
  root unchanged. A top-level `reference_data/` is reorg-stable for the same reason —
  it sits outside both `pipeline/` and `src/`, so the `pipeline/`→`src/` rename never
  touches it, and Dockerfiles reference it by a path that survives the move.

Dockerfiles pull it in exactly as they already pull the shared package
(`COPY utilities/ utilities/`): `COPY reference_data/basin <stage runtime path>`.

### Drift guard

Consolidation removes the *ability* to drift for the two moved assets (one copy
exists). No checksum test is required for them. Should a future asset need to stay
duplicated for a legitimate reason, a checksum-equality test over the copies is the
fallback (rejected here only because a single source is strictly better).

## The `basin_connection_table_v2.txt` header (implemented)

`basin_connection_table_v2.txt` has two consumers: `simple_gridder/gridding.py` reads
it internally, **and it is served on PO.DAAC as a reference resource**. The checked-in
file *is* the served file — the same artifact is uploaded and loaded (unlike the
indicator `.txt` products in `indicators/ref_files/txt_templates/`, which are header
*stubs* assembled with computed rows at delivery time). It just received a content
edit whose correctness (self-inclusion, no duplicates, symmetry) was verifiable only
by an ad-hoc script, not by anything the file recorded. **Decision: give it a
self-describing header, and fold it into the `reference_data/basin/` move so all
basin definitions live together.**

### Why HDR format (and how it differs from the indicator products)

Because the file is served publicly next to the NASA-SSH indicator files, its header
uses the same `HDR`-prefixed convention terminated by an `HDR Header_End` line, so it
reads consistently on the PO.DAAC shelf.

It deliberately carries **less** than the indicator headers, because it is
**reference material, not a product**. Products get the full apparatus — DOI,
citation block, `PLACEHOLDER_CREATION_DATE` injection, a `.mp` metadata sidecar
(`generate_mp`), a bounding box, a collection/version. A reference table has none of
those; its header carries only title, format, structural properties, and version.

Because the served file is public-facing, **internal references were kept out of the
header** (the loader class/path, "see ADR 0007", the `id >= 1000` guard as an
implementation detail). Those live here in the ADR; the file's header describes the
data to an external reader, not the code to an internal one.

This required a loader change, because the previous parse assumed every line was
data (`i, valid_is = line.split(":")` — a header line crashes it). Implemented:

- **Loader** — extracted the pure parse into a module-level
  `parse_basin_connections(lines)` in `gridding.py` that skips blank lines and
  `HDR`-prefixed header lines (resuming at the first data row); it keys off the `HDR`
  prefix rather than tracking the `Header_End` marker, which is simpler and equally
  correct given no data row starts with `HDR`. `Gridder.load_basin_connections` now
  just opens the file and delegates. Splitting parse from I/O also made it
  unit-testable without constructing a `Gridder` (which needs the mask files).
- **Header** — `HDR`-format block on `basin_connection_table_v2.txt`: title,
  description, format, the structural properties, and `Version 1.0` /
  `Last updated`. Provenance (source of the adjacency definition) is still open — see
  "Questions for review".
- **Tests** — `TestParseBasinConnections` covers plain rows, HDR-header + blank-line
  skipping, the `int16` dtype, and a real-file test that loads the shipped table
  through the parser and asserts the structural properties hold.

Note the header states structural properties but nothing *enforces* them at load
time — the real-file test is the guard. If the table is regenerated by tooling later,
that tooling (or the test) is the place to re-check symmetry/self-inclusion; the
loader deliberately stays a dumb parser. Keep the header's stated properties and the
test in sync when the table changes, since both are now public-facing claims.

## Consequences

- The two duplicated basin assets collapse to one canonical copy each; three stages
  (`simple_grids`, `daily_files`, `enso`) stop carrying redundant data.
- Regenerating the basin shapefile or quarter-degree mask becomes a single-file
  edit — the drift failure mode is eliminated by construction.
- Three Dockerfiles gain one `COPY` line each; no loader/path code changes for the
  moved assets. Image contents are unchanged (files still baked in at build), so no
  new runtime dependency is introduced.
- A new top-level `reference_data/` directory is introduced. ADR 0004 flagged caution
  about "introducing a new top-level layout days before the reorg reshuffles every
  path"; `reference_data/` is chosen precisely to *avoid* that churn — it lives
  outside both `pipeline/` and `src/`, so the reorg's `pipeline/`→`src/` rename leaves
  it untouched (the same reason `state_machines/` stays put).
- `gridding.py`'s loader now skips the `HDR` header and `basin_connection_table_v2.txt`
  carries a public-facing HDR header consistent with the PO.DAAC indicator files
  (done). When the directory move happens, the table joins `reference_data/basin/`,
  updating the one path string in the loader.

## Questions for review

1. Sequencing of the **directory move**: land it now against the current `pipeline/`
   layout, or defer to land *with* the reorg? `reference_data/` is reorg-stable
   either way, so landing now costs only the three `COPY` edits + one loader path
   string and does not add reorg work. (The header-tolerant loader and the table
   header are already in, independent of the move.)
2. **Provenance** of the adjacency definition — not stated in the header (unknown at
   authoring time). Decide whether to add a provenance/citation line to the HDR block
   before the next PO.DAAC upload, since the file is public-facing.
3. **Versioning convention.** Set to `Version 1.0` — the table was previously
   *unversioned*, so this is the initial versioned release, not a bump from a
   published 1.0. Future changes follow semver on the data: minor for
   backward-compatible content refinements, major for a format change. This is
   independent of the simple-grid *product* version (`v1_1` in output filenames) — a
   future table bump must not be read as a product bump.

## Implementation status

- **Done now** (safe ahead of the gridder build/deploy — no file moves, existing
  load path unchanged): `parse_basin_connections` in `gridding.py` skips the `HDR`
  header, the HDR-format header on `basin_connection_table_v2.txt`, and
  `TestParseBasinConnections` (4 cases, incl. real-file structural-property check).
  Full `simple_grids` suite green.
- **Pending**: create `reference_data/basin/`, `git mv` the shapefile set +
  `new_basin_mask_quartdeg.nc` + `basin_connection_table_v2.txt` into it, delete the
  duplicates, update the three Dockerfiles' `COPY` lines and the one loader path
  string. Deliberately deferred so it does not change the load path out from under
  an in-flight gridder build.
