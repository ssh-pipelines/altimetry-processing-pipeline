# ADR 0001: Adopt `pipeline_runtime` as the shared scientific-stack contract

- **Status**: Accepted
- **Date**: 2026-05-11

## Context

Each Lambda stage in the pipeline had its own `requirements.txt` pinning the
scientific stack independently. Over time these drifted:

| Package    | Versions in use                                     |
|------------|-----------------------------------------------------|
| `numpy`    | 1.26.2 (most stages) / 2.2.6 (enso only)            |
| `xarray`   | 2023.6.0 / 2023.10.1 / 2025.4.0                     |
| `netCDF4`  | 1.6.5 / 1.7.2                                       |
| `h5netcdf` | 1.3.0 / 1.6.1                                       |
| `scipy`    | 1.14.0 / 1.15.3                                     |

This drift was an **implicit, unowned contract**. Nothing in the repo said
"all stages that read or write daily files must agree on the encoding stack,"
but the daily-file format requires it: P1 → P2 → P3 → NASA-SSH hand off
NetCDF artifacts that the next stage opens, and the writer/reader libraries
must agree on encoding, attribute serialization, time decoding, and dtype
handling for the artifacts to round-trip correctly.

Beyond product validity, version drift made image builds and unit testing
harder: 10 places to bump a package, 8 venvs that could disagree with each
other or with prod.

## Decision

Introduce `pipeline_runtime` — a shared Docker base image (and a parallel
`pip install` layer for stage venvs) that pins the scientific stack once
for every stage that reads or writes a daily file.

**Layout** (under current `pipeline/`; survives the planned `src/` reorg):

```
pipeline/
  pipeline_runtime/
    Dockerfile          # FROM lambda/python:3.11; installs requirements.txt
    requirements.txt    # the contract — single source of truth for shared pins
    README.md
  daily_file_gen/<stage>/
    Dockerfile          # FROM ${BASE_REGISTRY}/pipeline_runtime:${BASE_TAG}
    requirements.txt    # stage-specific extras only
```

**Pinned base** (chosen 2026-05-11, validated against a known-good P3
fixture via the validation toolkit — max-abs numerical drift was 8.85e-15,
i.e. accumulated last-bit float noise, r=1.0000):

| Package   | Version  |
|-----------|----------|
| numpy     | 2.2.6    |
| xarray    | 2025.4.0 |
| netCDF4   | 1.7.2    |
| h5netcdf  | 1.6.1    |
| h5py      | 3.13.0   |
| scipy     | 1.15.3   |
| boto3     | 1.28.84  |
| s3fs      | (unpinned, current) |
| pyyaml    | (unpinned, current) |

### Sub-decisions

The following are load-bearing and should not be re-litigated without
revisiting this ADR.

**1. Lightweight Lambdas are deliberately excluded.**

`pipeline_init` and `unifier` do not consume `pipeline_runtime`. They touch
no NetCDF and stay on `lambda/python` directly. Pulling them onto the
scientific base would inflate their cold-start image from ~820 MB to over
1 GB for zero functional benefit, and would couple their build to every
runtime version bump.

**2. Version trains are coupled.**

Every artifact built in a single invocation of `scripts/{dev,prod}/build_and_push.sh`
shares one tag. Stages don't lag through a runtime upgrade individually —
when the runtime moves, every consumer rebuilds at the matching tag.

The tradeoff: if a runtime bump breaks one stage, the rollback is full
(redeploy every stage at the previous tag). Accepted because the goal of
the consolidation is to *prevent* the drift that lets one stage silently
diverge from the rest. The runtime image and stages live and die together.

**3. Dev tags use git short SHA; prod tags use semver from annotated tags.**

- `dev/<image>:dev-<sha>` — no version bookkeeping for dev iteration.
- `prod/<image>:<semver>` — `scripts/prod/build_and_push.sh` requires HEAD
  at an annotated tag `v<semver>`, clean working tree, and interactive
  confirmation. `PROD_CONFIRM=prod` bypasses the prompt for CI.

A prod release is `git tag -a v1.4.0` → `./scripts/prod/build_and_push.sh 1.4.0 ...`.
Every prod artifact is reproducible from a git tag.

**4. Stage Dockerfiles use build-arg parameterisation, not literal FROM.**

```dockerfile
ARG BASE_REGISTRY
ARG BASE_TAG
FROM ${BASE_REGISTRY}/pipeline_runtime:${BASE_TAG}
```

`BASE_REGISTRY` and `BASE_TAG` are set by the wrapper scripts. The same
Dockerfile is used for dev and prod; only the build args differ.

**5. `pyproject.toml` migration is out of scope.**

The Docker layering doesn't benefit from `pyproject.toml`'s optional
dependencies (the layered base + extras pattern intentionally re-uses the
base layer rather than reinstalling). The real case for `pyproject.toml`
is unified tool configuration (`[tool.ruff]`, `[tool.pytest]`), which is
orthogonal and best done in its own initiative across the whole repo.

## Consequences

**Positive**:

- One place to bump a scientific-stack version.
- Build artifacts are reproducible: a prod tag implies an exact runtime + stage
  set; `git checkout v1.4.0` rebuilds the bit-for-bit-equivalent image.
- Per-stage `requirements.txt` files shrink to just their extras, making
  stage dependencies easier to read.
- Unit tests run against the same scientific stack as prod, since the venv
  setup script (`.devcontainer/setup_python_venvs.sh`) layers the runtime
  requirements first.

**Negative**:

- A runtime bump becomes a coordinated change touching every consuming stage.
- The build script grows a precondition check (heavy stage → runtime tag
  must exist in ECR or be in the same invocation). One new failure mode if
  someone tries to rebuild a stage without building the runtime first; the
  script fails loudly with an actionable message.
- Lightweight stages still maintain their own pins (boto3, python-cmr, etc.),
  so a small amount of duplication remains there. Accepted because the cost
  of folding them in (cold-start, coupling) outweighs the savings.

## Alternatives considered

- **One image per Lambda, no shared base.** The current state. Rejected:
  the drift it produces is the problem this ADR resolves.
- **A "fat" base image including every stage's extras.** Rejected: stages
  that don't use cartopy/matplotlib would each carry ~400 MB they don't
  need, hitting the AWS Lambda 10 GB image limit faster and slowing cold
  starts.
- **`pyproject.toml` with optional dependencies (`pipeline_runtime[daily_files]`).**
  Rejected for this work (see sub-decision 5). Conceptually elegant but
  defeats the Docker layer-caching pattern.
- **`:latest` tag for the runtime instead of pinned per-build.** Rejected:
  loses reproducibility; a stage rebuild months later could pull a different
  base. Pinned tags survive in ECR for as long as the lifecycle policy keeps
  them, which is the source of reproducibility.
