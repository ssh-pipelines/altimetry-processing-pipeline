ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# bad_pass has no stage-specific deps; the `bad_pass` extra is empty and the
# scientific stack comes from the pipeline_runtime base. The export still
# captures the shared package's runtime deps (boto3/s3fs/pyyaml), already
# present in the base — a harmless no-op that keeps the build uniform.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --no-hashes --extra bad_pass \
        -o requirements.txt \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

# Shared package. Its deps are already satisfied above, so --no-deps keeps this
# a tiny layer that rebuilds only when utilities/ changes.
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY pipeline/daily_file_gen/bad_pass/bad_passes bad_passes/
COPY pipeline/daily_file_gen/bad_pass/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/bad_passes"

CMD [ "app.handler" ]
