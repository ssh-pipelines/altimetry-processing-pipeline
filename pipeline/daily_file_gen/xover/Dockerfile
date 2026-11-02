ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# Stage deps from the `xover` extra in the repo-root pyproject.toml, resolved
# via uv.lock. Exported + installed in a cached layer that only busts on
# pyproject.toml / uv.lock changes (not on stage or shared-package edits).
RUN --mount=type=bind,source=pyproject.toml,target=/tmp/lock/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/tmp/lock/uv.lock \
    uv export --project /tmp/lock --frozen --no-emit-project --no-hashes \
        --extra xover -o /tmp/requirements.txt \
    && pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

# Shared package. Its deps are already satisfied above, so --no-deps keeps this
# a tiny layer that rebuilds only when utilities/ changes.
COPY pyproject.toml ./
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY pipeline/daily_file_gen/xover/crossover crossover/
COPY pipeline/daily_file_gen/xover/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/crossover"

CMD [ "app.handler" ]
