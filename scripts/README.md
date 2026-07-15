# Deployment Pipelines

This directory contains the scripts that build, tag, push, and deploy the pipeline — both its Lambda **targets** and its Step Functions **state machine definitions** — for **development** and **production**. The workflow is tightly integrated with Git to ensure traceability, reproducibility, and minimal rebuild effort.

What each script manages — which targets exist, where they live, which are heavy (`FROM` pipeline_runtime), which map to a Lambda, and how they are packaged — comes from the **Target registry** (`utilities/targets.py` + `utilities/targets.yaml`), the single source of truth. State machines are not Targets (the registry does not track them); they are rendered from `state_machines/*.asl.json`. See `CONTEXT.md` → **Build & deploy** and `docs/adr/0004-target-registry.md`.

---

# Overview

```
scripts/
├── dev/
│   ├── build_and_push.sh     # Build + push container targets at git-SHA tags
│   ├── deploy.sh             # Deploy changed targets to the dev environment
│   └── pipeline.sh           # Orchestrates build, push, deploy (+ optional smoke) for dev
├── prod/
│   ├── build_and_push.sh     # Build + push container targets at RELEASE_VERSION tags (gated)
│   ├── deploy.sh             # Deploy targets to prod
│   ├── verify.sh             # Assert the live Lambdas match the released version
│   └── release.sh            # Full production release pipeline
├── state_machines/
│   ├── render.sh             # Render *.asl.json templates → rendered/ (substitutes ACCOUNT/REGION/STAGE)
│   ├── deploy.sh             # Deploy rendered definitions; stamp version; verify
│   └── rollback.sh           # Re-deploy a released version's definitions from its git tag
├── util/
│   ├── registry.sh           # Wrapper around the Target registry (registry_query)
│   ├── _build_and_push.sh    # Shared build/push core (container targets)
│   ├── _deploy.sh            # Shared deploy core (packaging seam: image vs zip)
│   ├── _verify.sh            # Shared post-deploy verification core (verify_targets)
│   ├── ecr_login.sh          # Authenticates Docker with AWS ECR
│   └── load_env.sh           # Loads shared environment variables
└── smoke.sh                  # Non-mutating end-to-end smoke test (fast-exit run + summary assert)
.env                          # File with deployment specific values
```

Requires a `.env` file at the repo root containing `AWS_REGION`, `AWS_ACCOUNT_ID`, `AWS_PROFILE` key/value pairs. `AWS_PROFILE` names the profile with valid credentials for `AWS_ACCOUNT_ID`'s ECR, Lambda, and Step Functions services. Optional values:

- `STATE_MACHINE_ROLE_ARN` — execution role ARN, used only when **creating** a state machine that doesn't exist yet (updates don't need it).
- `BUCKET_NAME` and `SMOKE_SOURCE` / `SMOKE_START` / `SMOKE_END` / `SMOKE_BUCKET` — defaults for the smoke test (see **Smoke test**).

The registry runs via `python3 -m utilities.targets`; the `utilities` package must be importable (`uv sync` from the repo root, or `pip install .`). If `python3` on PATH lacks it, set `REGISTRY_PY` (or `PYTHON`) to a venv interpreter (e.g. `.venv/bin/python`). `python3` (stdlib only) is also used to verify state-machine definitions and to assert the smoke run's summary.

Scripts are enforced to be run from the root of the repo.

---

# Development Pipeline

The **dev pipeline** is optimized for fast iteration. Images are tagged with the **current git SHA**, and only the images that have changed relative to `main` are rebuilt.

This provides:

- Fast incremental builds
- Full traceability to a commit
- Safe isolation of dev builds

## Running dev pipeline

```
scripts/dev/pipeline.sh [--all] [--dry-run] [--base <ref>] [--smoke]
```

### Options

| Flag | Description |
|------|-------------|
| `--all` | Build and deploy every target (and all state machines), bypassing change detection |
| `--dry-run` | Skips build, push, and deployment, logging steps to be taken instead |
| `--base <ref>` | Git ref to diff against for change detection (default: `main`) |
| `--smoke` | After deploying, run the non-mutating smoke test (params from the environment) |

### How the dev pipeline works

1. **Load environment + authenticate with ECR**
   Uses `load_env.sh` and `ecr_login.sh`.

2. **Detect which targets changed**
   Asks the Target registry: `registry_query dirty --base main`. Change-impact is dependency-aware — a change to `utilities/`, root `pyproject.toml`, or `pipeline_runtime/` dirties the stages that depend on them, not just the directory that changed. (`--all` lists the full catalog instead.)

3. **Detect state-machine changes**
   State machines aren't Targets, so they're gated separately: a deploy happens when a top-level `state_machines/*.asl.json` changed vs the base (or with `--all`).

4. **Build & push changed container targets**
   `dev/build_and_push.sh` builds the container targets (skipping zip targets, which have no image). The ECR repo is `dev/<image>`; the image tag is `dev-<git_sha>`.

5. **Deploy changed targets**
   `dev/deploy.sh` updates each deployable target's Lambda, branching on packaging kind: container targets via `--image-uri`, zip targets (the `pipeline/infra/` Lambdas) by zipping the source dir and using `--zip-file`. Both wait for the update to settle. Non-deployable targets (e.g. `pipeline_runtime`) are skipped.

6. **Render + deploy changed state machines**
   If step 3 found changes, render and deploy them to dev (after the Lambdas, so new orchestration points at code that already exists). Each machine is stamped with a `version` tag of the git SHA.

7. **Optional smoke** (`--smoke`)
   Run the end-to-end smoke test against an already-processed window (see **Smoke test**). Opt-in so routine pushes don't start an execution; skipped under `--dry-run`.

---

# Production Release Pipeline

The **prod pipeline** is designed for reproducible, auditable releases. Unlike dev, prod does **not** use git SHA tags. Instead, a release is explicitly tied to a **human-provided version** and an annotated git tag.

### Running a production release

```
scripts/prod/release.sh --version <RELEASE_VERSION> [--no-cleanup] [--dry-run]
```

### Required flag

| Flag | Description |
|-------|-------------|
| `--version <RELEASE_VERSION>` | Version to tag prod images with (ex: `2.2.0`). `build_and_push.sh` requires HEAD to be at the matching annotated tag `v<version>`. |

### Optional flags

| Flag | Description |
|------|-------------|
| `--no-cleanup` | Leaves dev + prod images on local machine |
| `--dry-run` | Prints actions without running them (and skips verification) |

A prod build is gated (in `prod/build_and_push.sh`): (1) clean working tree, (2) HEAD at an annotated tag matching `v<version>`, (3) interactive confirmation (skippable in CI with `PROD_CONFIRM=prod`).

---

## How the prod pipeline works

1. **Validate release version**
   Ensures a `--version` flag is provided.

2. **Load environment + authenticate**
   Same shared utility scripts as dev.

3. **Enumerate all targets**
   From the Target registry (`registry_query catalog`). Prod always rebuilds **every** container target and deploys **every** deployable target.

4. **Build & push all images**
   ECR repo `prod/<image>`, image tag `<RELEASE_VERSION>`.

5. **Deploy production Lambdas**
   `prod/deploy.sh` updates all prod functions to the release version.

6. **Verify the Lambda deploy**
   `prod/verify.sh` asserts every deployable target's live Lambda settled (State=Active, LastUpdateStatus=Successful) and runs the released image, before touching the state machines. Skipped in `--dry-run`.

7. **Render + deploy state machines**
   Renders `state_machines/*.asl.json` for prod and deploys them (after the Lambdas), stamping each with the release `version` tag and verifying the live definition matches what was rendered.

8. **Cleanup (default ON)**
   After a successful release, removes the prod image tagged with the release version and all dev-tagged images for each image.

---

# State Machines

State machine definitions are templates (`state_machines/*.asl.json`) with `${AWS_ACCOUNT_ID}`, `${AWS_REGION}`, and `${STAGE}` placeholders. They are deployed as part of `release.sh` (prod) and `pipeline.sh` (dev, when an ASL changed), or directly:

```
scripts/state_machines/render.sh --stage <dev|prod> [--file <name>.asl.json]
scripts/state_machines/deploy.sh --stage <dev|prod> [--file <name>.asl.json] [--version <v>] [--dry-run]
```

`deploy.sh` creates the machine if it doesn't exist (needs `STATE_MACHINE_ROLE_ARN`), else updates it in place, then — when `--version` is given — stamps a `version` resource tag and verifies the live definition matches the rendered file.

## Rollback

Unlike Lambda images (every version persists in ECR, so a rollback is just `update-function-code --image-uri <repo>:<old>`), Step Functions **overwrites its definition in place** — there is no per-version artifact. The annotated git tag `vX.Y.Z` is therefore the version-keyed source of truth: rendering that tag's templates reproduces, byte-for-byte, what the release deployed.

```
scripts/state_machines/rollback.sh --version <X.Y.Z> --stage <dev|prod> [--file <name>.asl.json] [--dry-run]
```

It shallow-clones the tag into a tmp dir (your working tree is untouched), renders that tag's definitions, and deploys them with the **current** tooling — so a rollback can't drag along an old release's deploy bugs. Use `--dry-run` first. To see what's live, read the `version` tag: `aws stepfunctions list-tags-for-resource --resource-arn <sm-arn>`.

---

# Smoke test

`scripts/smoke.sh` is a **non-mutating** end-to-end check. It starts a real pipeline execution over an already-processed window **without** `force_update`, so `pipeline_init` finds no new jobs and the run fast-exits straight to `run_summary` — exercising the whole orchestration (`pipeline_init`, the `Has Jobs?`/`No Jobs` wiring, `run_summary`, the success notification) while writing **zero product data**. It then asserts the run summary reconciles clean (every product pipeline `expected == produced`, `missing == []`).

```
scripts/smoke.sh --source <NAME> --start <YYYY-MM-DD> --end <YYYY-MM-DD> \
                 [--stage dev|prod] [--bucket <b>] [--timeout <sec>] [--no-precheck]
```

Pick a window you know is already processed (or has no data). By default a pre-check invokes `pipeline_init` and **aborts if the window has new jobs** (which would process real data); `--no-precheck` overrides. `--bucket` defaults to `BUCKET_NAME`. The dev pipeline runs this via `pipeline.sh --smoke`, drawing `--source`/`--start`/`--end` from `SMOKE_SOURCE` / `SMOKE_START` / `SMOKE_END` (and `SMOKE_BUCKET`). Run it right after a deploy so the weekly scheduled run is a confirmation, not your first signal.

---

# Utilities

## `util/registry.sh`
Wrapper exposing `registry_query <subcommand>` over the Target registry (`utilities/targets.py`). Used by every orchestrator and core to ask which targets exist, where they live, which are heavy/deployable, and which changed (`catalog` / `dirty`).

## `util/_build_and_push.sh`
Shared build/push core. Builds and pushes **container** targets (orders `pipeline_runtime` first, passes `BASE_IMAGE` to heavy stages, ensures the ECR repo exists). Skips zip targets.

## `util/_deploy.sh`
Shared deploy core (`deploy_targets`). Deploys each deployable target at the packaging seam: container → `update-function-code --image-uri`, zip → zip the source dir → `update-function-code --zip-file`. Both wait for the update to settle.

## `util/_verify.sh`
Shared verification core (`verify_targets`). For each deployable target, asserts the live Lambda settled (State=Active, LastUpdateStatus=Successful) and runs the expected artifact (container: live image URI == `<repo>:<tag>`). Returns non-zero on any mismatch.

## `util/ecr_login.sh`
Authenticates Docker with AWS ECR and returns the registry URI.

## `util/load_env.sh`
Loads shared environment variables (AWS account ID, region, profile, git SHA) and disables the AWS CLI v2 pager. Used by all build/deploy scripts.

---

# Summary

### Dev pipeline (`dev/pipeline.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `dev/<image>:dev-<git_sha>` |
| Builds | Only changed targets (dependency-aware) |
| Deploys | Only changed targets (container + zip) + changed state machines |
| Smoke | Opt-in via `--smoke` (params from `SMOKE_*` env) |
| Flags | `--all`, `--dry-run`, `--base <ref>`, `--smoke` |

### Prod pipeline (`prod/release.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `prod/<image>:<RELEASE_VERSION>` |
| Builds | All container targets, always |
| Deploys | All deployable targets (container + zip) + all state machines |
| Verifies | Lambda images + state-machine definitions match the release |
| Cleanup | ON by default (removes dev+prod images locally) |
| Flags | `--version`, `--no-cleanup`, `--dry-run` |

### Rollback
| Artifact | Mechanism |
|----------|-----------|
| Lambdas | Persist per-version in ECR → `update-function-code --image-uri <repo>:<old>` |
| State machines | `state_machines/rollback.sh --version <X.Y.Z>` (re-render from the git tag) |
