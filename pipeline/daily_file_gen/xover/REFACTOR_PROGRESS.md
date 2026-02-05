# Xover Module Refactor - Progress Tracker

## Current Status: Phase 3 Complete

### Test Results (21/21 pass)
- 13 consistency tests (ConsistencyTestCase): PASS
- 3 empty input tests (EmptyInputTestCase): PASS
- 5 source config tests (TestSourceConfig): PASS

---

## Phase 1: Test Suite (COMPLETE)

### Files Created/Modified
| File | Notes |
|------|-------|
| `tests/test_crossover.py` | ConsistencyTestCase + EmptyInputTestCase |
| `tests/test_source_config.py` | Config system tests |
| `tests/__init__.py` | Added repo root to sys.path (matches daily_files pattern) |

### Notes
- Time tests use int64 comparison (not `assert_allclose`) to avoid float64 precision loss on large nanosecond timestamps (~1.7e18)
- 200ns tolerance accounts for 128ns max observed diff from interpolation rounding

---

## Phase 2: Configuration System (COMPLETE)

### Files Created
| File | Notes |
|------|-------|
| `crossover/config/__init__.py` | Package init |
| `crossover/config/sources.yaml` | GSFC + S6 orbital parameters |
| `crossover/config/source_config.py` | SourceConfig dataclass, get_source_config(), get_available_sources() |

### Files Modified
| File | Changes |
|------|---------|
| `crossover/parallel_crossovers.py` | Replaced hardcoded WINDOW_SIZE, WINDOW_PADDING, CYCLE_LENGTH, MAX_DIFF with `get_source_config()`. Kept EPOCH and ZERO_DIFF as module constants. |
| `requirements.txt` | Added pyyaml |

---

## Phase 3: Efficiency Improvements (COMPLETE)

### Changes Made

1. **Track start calculation: O(n*m) -> O(n log n)** — Replaced per-track `np.min()` list comprehension with `np.lexsort` + group boundary detection in `extract_and_set_data()`.

2. **Pre-built track index** — Added `_build_track_index()` that maps each track ID to its array indices via a dict. `get_track_data()` now uses direct index lookup instead of scanning the full array with `==` on every call.

3. **`drop_variables` on file open** — Moved variable exclusion from `ds.drop_vars()` after loading to `drop_variables=` parameter in `xr.open_dataset()`, avoiding deserialization of unused variables.

4. **Test data compression** — Sample data files stored as `.nc.gz` (28 MB -> 9 MB). Input granules stripped to only the 6 variables used by the code. Tests decompress to a temp directory automatically.

### Not included
- `stream_files()` optimization (S3 glob -> direct path construction) — production-only code not covered by consistency tests. Can be done separately.

---

## Verification

```bash
cd pipeline/daily_file_gen/xover
source .venv/bin/activate
python -m unittest discover -s tests -t . -v
```

All 21 tests must pass.
