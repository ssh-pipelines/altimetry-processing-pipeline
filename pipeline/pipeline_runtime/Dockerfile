# Shared scientific-stack base image. Heavy stages FROM this tag.
#
# Dependencies come from the `pipeline_runtime` extra in the repo-root
# pyproject.toml (single source of truth), resolved through uv.lock. We export
# the locked deps to a requirements file and pip-install them in one cached
# layer that only busts when pyproject.toml / uv.lock change — not when stage
# code or the shared utilities package changes.
FROM public.ecr.aws/lambda/python:3.11

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

#
# The lockfile is bind-mounted rather than copied, and the export goes to /tmp,
# so neither lands in the image: it lists every stage's deps, and ECR scanning
# reports each one against any image that carries it, installed or not. Stage
# images inherit this filesystem, so the same holds for all of them.
RUN --mount=type=bind,source=pyproject.toml,target=/tmp/lock/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/tmp/lock/uv.lock \
    uv export --project /tmp/lock --frozen --no-dev --no-emit-project --no-hashes \
        --extra pipeline_runtime -o /tmp/requirements.txt \
    && pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
