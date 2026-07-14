ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

ARG ROOT_DIR=pipeline/other_products/enso
ARG CARTOPY_DATA_DIR=/opt/cartopy_data

ENV CARTOPY_OFFLINE=true
ENV CARTOPY_DATA_DIR=$CARTOPY_DATA_DIR
ENV MPLCONFIGDIR=/tmp/matplotlib

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /bin/uv

# Stage deps from the `enso` extra in the repo-root pyproject.toml, resolved
# via uv.lock. Exported + installed in a cached layer that only busts on
# pyproject.toml / uv.lock changes (not on stage or shared-package edits).
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-emit-project --no-hashes --extra enso \
        -o requirements.txt \
    && pip install --upgrade pip \
    && pip install -r requirements.txt

# Create cartopy data directory and set permissions
RUN mkdir -p $CARTOPY_DATA_DIR && chmod -R a+rwx $CARTOPY_DATA_DIR

# Copy setup_cartopy.py into the container
COPY $ROOT_DIR/cartopy_setup.py .

# Run the setup_cartopy.py script
RUN python cartopy_setup.py

# Shared package. Its deps are already satisfied above, so --no-deps keeps this
# a tiny layer that rebuilds only when utilities/ changes.
COPY utilities/ utilities/
RUN pip install --no-deps .

COPY $ROOT_DIR/. ${LAMBDA_TASK_ROOT}

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/enso"

CMD [ "app.handler" ]