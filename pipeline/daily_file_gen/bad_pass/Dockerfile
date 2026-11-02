ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# bad_pass has no stage-specific deps; the `bad_pass` extra is empty and the
# scientific stack comes from the pipeline_runtime base. The export still
# captures the shared package's runtime deps (boto3/s3fs/pyyaml), already
# present in the base — a harmless no-op that keeps the build uniform.
RUN --mount=type=bind,source=pyproject.toml,target=/tmp/lock/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/tmp/lock/uv.lock \
    uv export --project /tmp/lock --frozen --no-emit-project --no-hashes \
        --extra bad_pass -o /tmp/requirements.txt \
    && pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

# Shared package. Its deps are already satisfied above, so --no-deps keeps this
# a tiny layer that rebuilds only when utilities/ changes.
COPY pyproject.toml ./
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY pipeline/daily_file_gen/bad_pass/bad_passes bad_passes/
COPY pipeline/daily_file_gen/bad_pass/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/bad_passes"

CMD [ "app.handler" ]
