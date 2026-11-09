# Build context must be root nasa-ssh-pipeline directory

# Global ARG (declared before any FROM) so it's visible in the final FROM
# below, which spans multiple build stages.
ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.12

FROM alpine AS intermediate

COPY pipeline/daily_file_gen/daily_files/daily_files/ref_files /tmp/ref_files

# Compile the orbit-swap C executable against netCDF
FROM public.ecr.aws/lambda/python:3.12 AS c_builder

# Libraries the base image already ships, recorded before anything is installed.
RUN ls /lib64 > /tmp/base_libs

# AL2023 has no EPEL; netcdf-devel comes from SPAL, Amazon's rebuild of EPEL9.
RUN dnf install -y spal-release && \
    dnf install -y gcc make netcdf-devel

COPY pipeline/daily_file_gen/daily_files/daily_files/ingestion/interpPosGoaToNetCDF/ \
    /tmp/interpPosGoaToNetCDF/

RUN cd /tmp/interpPosGoaToNetCDF && \
    make clean && \
    make CFLAGS="-O3 -c"

# Collect the shared libraries the binary needs that the base image lacks
# (netcdf, hdf5, hdf4, szip). Ones the base already has (openssl, curl, krb5)
# are left out: LD_LIBRARY_PATH points at the copies, so they would shadow the
# runtime's own for Python as well. The runtime is FROM the same lambda/python
# tag as this stage, so the base list holds there too.
RUN mkdir /tmp/orbit_libs && \
    ldd /tmp/interpPosGoaToNetCDF/interpPosGoaToNetCDFtimes.e \
    | grep "=> /" \
    | awk '{print $3}' \
    | while read -r lib; do \
        grep -qxF "$(basename "$lib")" /tmp/base_libs || cp "$lib" /tmp/orbit_libs/; \
      done

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

# Copy the compiled orbit-swap binary and its bundled shared libraries
RUN mkdir -p ${LAMBDA_TASK_ROOT}/bin ${LAMBDA_TASK_ROOT}/libs
COPY --from=c_builder /tmp/interpPosGoaToNetCDF/interpPosGoaToNetCDFtimes.e ${LAMBDA_TASK_ROOT}/bin/
COPY --from=c_builder /tmp/orbit_libs/ ${LAMBDA_TASK_ROOT}/libs/

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pyproject.toml ./
COPY utilities/ utilities/
RUN pip install --no-deps .

# Copy other files from the local directory
COPY pipeline/daily_file_gen/daily_files/daily_files daily_files/
COPY pipeline/daily_file_gen/daily_files/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/daily_files"
ENV LD_LIBRARY_PATH="${LAMBDA_TASK_ROOT}/libs"

CMD [ "app.handler" ]