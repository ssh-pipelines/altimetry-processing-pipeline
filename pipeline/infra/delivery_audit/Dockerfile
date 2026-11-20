FROM public.ecr.aws/lambda/python:3.12

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# Stage deps from the `delivery_audit` extra in the repo-root pyproject.toml,
# resolved via uv.lock, in a layer that only busts on pyproject.toml / uv.lock.
RUN --mount=type=bind,source=pyproject.toml,target=/tmp/lock/pyproject.toml \
    --mount=type=bind,source=uv.lock,target=/tmp/lock/uv.lock \
    uv export --project /tmp/lock --frozen --no-dev --no-emit-project --no-hashes \
        --extra delivery_audit -o /tmp/requirements.txt \
    && pip install --upgrade pip \
    && pip install -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

# Shared package, for the delivery declaration in products.yaml and the
# products/ layout. Its deps are already satisfied above.
COPY pyproject.toml ./
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY pipeline/infra/delivery_audit/app.py .
COPY pipeline/infra/delivery_audit/audit.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
