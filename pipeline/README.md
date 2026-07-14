# Building images

Images must be built from the above root context in order to properly install the utilities package. Each module directory contains a `Dockerfile`; its dependencies come from that stage's optional-dependency extra in the root `pyproject.toml` (exported via `uv` at build time), so there is no per-module `requirements.txt`.