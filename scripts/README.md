# Deployment Pipelines

This directory contains scripts used to build, tag, push, and deploy Docker images for both **development** and **production** environments. The workflow is tightly integrated with Git to ensure traceability, reproducibility, and minimal rebuild effort.

---

# Overview

```
scripts/
├── dev/
│   ├── build_and_push.sh     # Build + push images using git SHA tags
│   ├── deploy.sh             # Update dev environment to use pushed images
│   └── pipeline.sh           # Orchestrates build, push, deploy for dev
├── prod/
│   ├── build_and_push.sh     # Build + push images using RELEASE_VERSION tags
│   ├── deploy.sh             # Deploy prod images
│   └── release.sh            # Full production release pipeline
└── util/
    ├── ecr_login.sh          # Authenticates Docker with AWS ECR
    ├── find_images.sh        # Enumerates all images in the monorepo
    └── load_env.sh           # Loads shared environment variables
.env                          # File with deployment specific values
```

Requires `.env` file at the repo root containg `AWS_REGION`,  `AWS_ACCOUNT_ID`, `AWS_PROFILE` key value pairs. `AWS_PROFILE` contains the profile name to use with valid credentials for accessing `AWS_ACCOUNT_ID`'s ECR and Lambda services with sufficient privelege   

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
scripts/dev/pipeline.sh [--all] [--dry-run]
```

### Options

| Flag | Description |
|------|-------------|
| `--all` | Forces build and deploy of all images within repo, bypassing git diff with main |
| `--dry-run` | Skips build, push, and deployment, logging steps to be taken instead |

### How the dev pipeline works

1. **Load environment + authenticate with ECR**  
   Uses `load_env.sh` and `ecr_login.sh`.

2. **Generate list of all images**  
   Uses `util/find_images.sh`.

3. **Detect which images changed**  
   For each image directory, the pipeline compares:
   ```
   git diff main..HEAD
   ```
   Only changed images are rebuilt.

4. **Build & push changed images**  
   Tags are of the form:
   ```
   <registry>/dev/<image>:<git_sha>
   ```

5. **Deploy updated images**  
   The `dev/deploy.sh` script updates Lambda functions, ECS tasks, or other resources to pull the new dev-tagged image.

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

3. **Enumerate all images**  
   Prod always rebuilds **every** image.

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

## `util/find_images.sh`
Outputs the canonical list of Docker image directories.  
Both dev and prod orchestrators use this to iterate through the full set of images.

## `util/ecr_login.sh`
Authenticates Docker with AWS ECR and returns the registry URI.

## `util/load_env.sh`
Loads shared environment variables (AWS account ID, region, repository name, etc.).  
Used by all build/deploy scripts.

---

# Summary

### Dev pipeline (`dev/pipeline.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `dev/<image>:<git_sha>` |
| Builds | Only changed images |
| Deploys | Only rebuilt images |
| Flags | `--all`, `--dry-run` |

### Prod pipeline (`prod/release.sh`)
| Feature | Behavior |
|---------|----------|
| Tag format | `prod/<image>:<RELEASE_VERSION>` |
| Builds | All images, always |
| Deploys | Entire environment |
| Cleanup | ON by default (removes dev+prod images locally) |
| Flags | `--version`, `--no-cleanup`, `--dry-run` |
