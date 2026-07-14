# ADR 0004: Target registry as the single source of truth for build/deploy

- **Status**: Accepted
- **Date**: 2026-06-04

## Context

The build/deploy scripts (`scripts/dev/`, `scripts/prod/`, `scripts/util/`) had no single source of truth for what they manage. The knowledge of each build target was scattered:

- **Existence + location** — re-derived by `find … -name "$IMAGE" | head -1` in both `dev/pipeline.sh` and `util/_build_and_push.sh`, and by `util/find_images.sh` walking Dockerfiles.
- **Which images are "heavy"** (i.e. `FROM` pipeline_runtime via the `BASE_IMAGE` override) — a hardcoded `HEAVY_STAGES=(…)` array in `_build_and_push.sh`.
- **Which images have no Lambda** — a hardcoded `BUILD_ONLY_IMAGES=(pipeline_runtime)` array duplicated in `dev/deploy.sh` and `prod/deploy.sh`.
- **Naming conventions** — `${stage}/${name}` (ECR repo) and `${stage}-${name}` (function) re-spelled across three scripts.

Two concrete consequences:

1. **Stale dev deploys.** The dev loop rebuilds an image only if *its own directory* changed vs `main` (`git diff -- "$DIR"`). But every heavy stage `FROM`s pipeline_runtime and every stage `pip install`s the shared `utilities` package. A change to `utilities/`, root `setup.py`, or `pipeline_runtime/` changes the *content* of those images but touches no stage directory, so the loop silently skips the rebuild and deploys stale code on a green run.
2. **Infra Lambdas had no deploy path.** The `pipeline/infra/` Lambdas (`failure_handling`, `podaac_auth`, `rewrite_manifest`, `set_sg_jobs`) are zip-packaged, not containerized. The scripts only ever handled container images, so these were deployed by hand.

This is an **implicit, unowned contract** in the same spirit as the scientific-stack drift that motivated ADR 0001 — the knowledge of "what gets built and deployed, and how" lived in `find` calls and hand-maintained shell arrays with no test surface.

## Decision

Introduce a **Target registry** (`utilities/targets.py`, `utilities/targets.yaml`) as the one module the build/deploy scripts query. See CONTEXT.md → **Build & deploy** for the full vocabulary.

- A **Target** is anything the scripts manage — a buildable image and/or a deployable Lambda.
- **Existence** and **packaging kind** (`container` vs `zip`) are **derived from the filesystem**: a Dockerfile ⇒ `container`; an `app.py` under `pipeline/infra/` with no Dockerfile ⇒ `zip`.
- The two facts that can't be reliably derived — **`heavy`** (brittle to grep for; see below) and **`deployable`** (a fact about what exists in AWS) — are **declared** in `targets.yaml`. The manifest is **path-less** so it survives the planned `pipeline/`→`src/` reorganization untouched.
- `targets()` **raises if the manifest and filesystem disagree**, so every caller (not just the test) is protected from drift.
- The registry exposes the catalog (with `ecr_repo` / `function_name` helpers) and a pure **change-impact** function (`dirty(changed_paths)`) encoding the real dependency edges: a container stage is dirty if its own dir, shared `utilities/` + `pyproject.toml`, or (if heavy) `pipeline_runtime/` changed; a zip target is dirty only if its own dir changed.

The build/deploy phases become filters over one catalog, with a **packaging seam**: build/push handles `container` targets; deploy iterates `deployable` targets and branches on packaging (`--image-uri` vs zip + `--zip-file`). This is where the infra Lambdas enter the loop.

### Why declare `heavy` rather than derive it

"Heavy" *is* derivable — a heavy image's Dockerfile parameterizes its base (`ARG BASE_IMAGE` / `FROM ${BASE_IMAGE}`) while lightweight ones use a literal `FROM`. But that derivation is a fragile grep contract: a Dockerfile reformat could silently reclassify a stage and build it on the wrong base. We declare `heavy` in `targets.yaml` and **guard the grep contract with a test** (`heavy` ⟺ Dockerfile parameterizes `BASE_IMAGE`), turning silent drift into a caught failure.

## Consequences

- `find_images.sh`, the `HEAVY_STAGES` / `BUILD_ONLY_IMAGES` arrays, the duplicated `find … -name` lookups (and their `| head -1` ambiguity), and the inline `git diff` gate all collapse into registry calls.
- The change-impact logic gains a **test surface** for the first time: consistency, edge correctness, packaging derivation, and naming helpers are all unit-testable.
- Reorg-robustness is concentrated in two places: the path-less manifest, and a single `_SHARED_BUILD_PATHS` constant in the module (the `utilities/`→`src/shared/` move updates one line).
- The scripts now shell out to Python (`python -m utilities.targets …`). This is not a new dependency — the package is already installed for builds and tests.

## Placement (and intended end-state)

The registry lives in `utilities/` **for now**, chosen for least friction: that package already provided a free importable module (`python -m utilities.targets`), a `tests/` harness, and CI coverage — the test surface this design depends on. Standing up an equivalent from scratch in a new directory would have meant re-creating that packaging/import/CI plumbing.

This is, however, a known compromise: the registry is **build-time tooling, not runtime**, but every stage Dockerfile does `COPY utilities/ … && pip install .`, so `targets.py` / `targets.yaml` currently get baked into every Lambda image. Harmless in size, but the wrong dependency direction (build tooling shipped inside the runtime artifact), and it splits the build subsystem across `utilities/` (registry) and `scripts/` (the scripts that consume it).

**Intended end-state:** relocate the registry + scripts into a dedicated build package, out of the shipped runtime, to **land with the directory reorganization** (`REORGANIZATION_PLAN.md` → "Build tooling placement"). The registry must remain an importable, test-covered package so the consistency/change-impact drift guard keeps running in CI; the code touch-ups are small (module path in `registry.sh`, the `pipeline` walk roots and `_SHARED_BUILD_PATHS` constants in `targets.py`, `setup.py` packaging). Deferred rather than done now to avoid introducing a new top-level layout days before the reorg reshuffles every path.
