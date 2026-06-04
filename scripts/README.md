# Deployment Pipelines

This directory contains scripts used to build, tag, push, and deploy the pipeline's Lambda **targets** for both **development** and **production** environments. The workflow is tightly integrated with Git to ensure traceability, reproducibility, and minimal rebuild effort.

What each script manages — which targets exist, where they live, which are heavy (`FROM` pipeline_runtime), which map to a Lambda, and how they are packaged — comes from the **Target registry** (`utilities/targets.py` + `utilities/targets.yaml`), the single source of truth. See `CONTEXT.md` → **Build & deploy** and `docs/adr/0004-target-registry.md`.

---

# Overview

```
scripts/
├── dev/
│   ├── build_and_push.sh     # Build + push container targets at git-SHA tags
│   ├── deploy.sh             # Deploy changed targets to the dev environment
│   └── pipeline.sh           # Orchestrates build, push, deploy for dev
├── prod/
│   ├── build_and_push.sh     # Build + push container targets at RELEASE_VERSION tags
│   ├── deploy.sh             # Deploy targets to prod
│   └── release.sh            # Full production release pipeline
└── util/
    ├── registry.sh           # Wrapper around the Target registry (registry_query)
    ├── _build_and_push.sh    # Shared build/push core (container targets)
    ├── _deploy.sh            # Shared deploy core (packaging seam: image vs zip)
    ├── ecr_login.sh          # Authenticates Docker with AWS ECR
    └── load_env.sh           # Loads shared environment variables
.env                          # File with deployment specific values
```

Requires a `.env` file at the repo root containing `AWS_REGION`, `AWS_ACCOUNT_ID`, `AWS_PROFILE` key/value pairs. `AWS_PROFILE` names the profile with valid credentials for `AWS_ACCOUNT_ID`'s ECR and Lambda services.

The registry runs via `python3 -m utilities.targets`; the `utilities` package must be importable (`pip install .` from the repo root). If `python3` on PATH lacks it, set `REGISTRY_PY` (or `PYTHON`) to a venv interpreter.

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
scripts/dev/pipeline.sh [--all] [--dry-run] [--base <ref>]
```

### Options

| Flag | Description |
|------|-------------|
| `--all` | Build and deploy every target, bypassing change detection |
| `--dry-run` | Skips build, push, and deployment, logging steps to be taken instead |
| `--base <ref>` | Git ref to diff against for change detection (default: `main`) |

### How the dev pipeline works

1. **Load environment + authenticate with ECR**  
   Uses `load_env.sh` and `ecr_login.sh`.

2. **Detect which targets changed**  
   Asks the Target registry: `registry_query dirty --base main`. Change-impact is dependency-aware — a change to `utilities/`, root `setup.py`, or `pipeline_runtime/` dirties the stages that depend on them, not just the directory that changed. (`--all` lists the full catalog instead.)

3. **Build & push changed container targets**  
   `dev/build_and_push.sh` builds the container targets (skipping zip targets, which have no image). Tags are of the form:
   ```
   <registry>/dev/<image>:<git_sha>
   ```

4. **Deploy changed targets**  
   `dev/deploy.sh` updates each deployable target's Lambda, branching on packaging kind: container targets via `--image-uri`, zip targets (the `pipeline/infra/` Lambdas) by zipping the source dir and using `--zip-file`. Non-deployable targets (e.g. `pipeline_runtime`) are skipped.

---

# Production Release Pipeline

The **prod pipeline** is designed for reproducible, auditable releases. Unlike dev, prod does **not** use git SHA tags. Instead, a release is explicitly tied to a **human-provided version**.

### Running a production release

```
scripts/prod/release.sh --version <RELEASE_VERSION> [--no-cleanup] [--dry-run]
```

### Required flag

| Flag | Description |
|-------|-------------|
| `--version <RELEASE_VERSION>` | Version to tag prod images with (ex: `v1.7.0`, `2025.03`) |

### Optional flags

| Flag | Description |
|------|-------------|
| `--no-cleanup` | Leaves dev + prod images on local machine |
| `--dry-run` | Prints actions without running them |

---

## How the prod pipeline works

1. **Validate release version**  
   Ensures a `--version` flag is provided.

2. **Load environment + authenticate**  
   Same shared utility scripts as dev.

3. **Enumerate all targets**  
   From the Target registry (`registry_query catalog`). Prod always rebuilds **every** container target and deploys **every** deployable target.

4. **Build & push all images**  
   Tags are of the form:
   ```
   <registry>/prod/<image>:<RELEASE_VERSION>
   ```

5. **Deploy production environment**  
   `prod/deploy.sh` updates all prod services to the release version.

6. **Cleanup (default ON)**  
   After a successful release, the script removes:

   **Prod images tagged with the release version**  
   ```
   <registry>/prod/<image>:<RELEASE_VERSION>
   ```

   **All dev-tagged images for each image**
   ```
   <registry>/dev/<image>:<any_sha>
   ```

---

# Utilities

## `util/registry.sh`
Wrapper exposing `registry_query <subcommand>` over the Target registry (`utilities/targets.py`). Used by every orchestrator and core to ask which targets exist, where they live, which are heavy/deployable, and which changed (`catalog` / `dirty`).

## `util/_build_and_push.sh`
Shared build/push core. Builds and pushes **container** targets (orders `pipeline_runtime` first, passes `BASE_IMAGE` to heavy stages, ensures the ECR repo exists). Skips zip targets.

## `util/_deploy.sh`
Shared deploy core (`deploy_targets`). Deploys each deployable target at the packaging seam: container → `update-function-code --image-uri`, zip → zip the source dir → `update-function-code --zip-file`.

## `util/ecr_login.sh`
Authenticates Docker with AWS ECR and returns the registry URI.

## `util/load_env.sh`
Loads shared environment variables (AWS account ID, region, profile, git SHA).  
Used by all build/deploy scripts.

---

# Summary

### Dev pipeline (`dev/pipeline.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `dev/<image>:<git_sha>` |
| Builds | Only changed targets (dependency-aware) |
| Deploys | Only changed targets (container + zip) |
| Flags | `--all`, `--dry-run`, `--base <ref>` |

### Prod pipeline (`prod/release.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `prod/<image>:<RELEASE_VERSION>` |
| Builds | All container targets, always |
| Deploys | All deployable targets (container + zip) |
| Cleanup | ON by default (removes dev+prod images locally) |
| Flags | `--version`, `--no-cleanup`, `--dry-run` |
