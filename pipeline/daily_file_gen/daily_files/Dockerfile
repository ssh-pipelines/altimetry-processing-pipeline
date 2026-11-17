# Build context must be root nasa-ssh-pipeline directory

# Global ARG (declared before any FROM) so it's visible in the final FROM
# below, which spans multiple build stages.
ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.12

FROM alpine AS intermediate

COPY pipeline/daily_file_gen/daily_files/daily_files/ref_files /tmp/ref_files

FROM ${BASE_IMAGE}

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# Stage deps from the `daily_files` extra in the repo-root pyproject.toml,
# resolved via uv.lock. Exported + installed in a cached layer that only busts
# on pyproject.toml / uv.lock changes (not on stage or shared-package edits).
RUN --mount=type=bind,source=pyproject.toml,target=/tmp/lock/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/tmp/lock/uv.lock \
    uv export --project /tmp/lock --frozen --no-dev --no-emit-project --no-hashes \
        --extra daily_files -o /tmp/requirements.txt \
    && pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

# Copy large files from the intermediate stage
COPY --from=intermediate /tmp/ref_files ${LAMBDA_TASK_ROOT}/daily_files/ref_files

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject.toml ./
COPY utilities/ utilities/
RUN pip install --no-deps .

# Copy other files from the local directory
COPY pipeline/daily_file_gen/daily_files/daily_files daily_files/
COPY pipeline/daily_file_gen/daily_files/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/daily_files"

CMD [ "app.handler" ]