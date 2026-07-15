# Shared scientific-stack base image. Heavy stages FROM this tag.
#
# Dependencies come from the `pipeline_runtime` extra in the repo-root
# pyproject.toml (single source of truth), resolved through uv.lock. We export
# the locked deps to a requirements file and pip-install them in one cached
# layer that only busts when pyproject.toml / uv.lock change — not when stage
# code or the shared utilities package changes.
FROM public.ecr.aws/lambda/python:3.11

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --no-hashes --extra pipeline_runtime \
        -o requirements.txt \
    && pip install --upgrade pip \
    && pip install -r requirements.txt
