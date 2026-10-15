FROM public.ecr.aws/lambda/python:3.11

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# Stage deps from the `start_run` extra in the repo-root pyproject.toml,
# resolved via uv.lock. Exported + installed in a cached layer that only busts
# on pyproject.toml / uv.lock changes (not on stage or shared-package edits).
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --no-hashes --extra start_run \
        -o requirements.txt \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

# Shared package. This stage exists *because* of it: resolving contributors
# means reading the Source registry, which ASL cannot do and a zip infra Lambda
# does not ship. Its deps are already satisfied above, so --no-deps keeps this a
# tiny layer that rebuilds only when utilities/ changes.
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY pipeline/infra/start_run/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
