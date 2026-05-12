# Build context must be root nasa-ssh-pipeline directory

# Global ARG (declared before any FROM) so it's visible in the final FROM
# below, which spans multiple build stages.
ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11

FROM alpine AS intermediate

COPY pipeline/daily_file_gen/daily_files/daily_files/ref_files /tmp/ref_files

# Compile the orbit-swap C executable against netCDF
FROM public.ecr.aws/lambda/python:3.11 AS c_builder

RUN yum install -y gcc make amazon-linux-extras && \
    PATH=/usr/bin:/usr/sbin:/bin:/sbin amazon-linux-extras install -y epel && \
    yum install -y netcdf-devel

COPY pipeline/daily_file_gen/daily_files/daily_files/ingestion/interpPosGoaToNetCDF/ \
    /tmp/interpPosGoaToNetCDF/

RUN cd /tmp/interpPosGoaToNetCDF && \
    make clean && \
    make CFLAGS="-O3 -c -I/usr/include/netcdf"

# Collect shared libraries needed by the binary (netcdf, hdf5, etc.).
# Excludes standard glibc libs that are always present in the runtime image.
RUN mkdir /tmp/orbit_libs && \
    ldd /tmp/interpPosGoaToNetCDF/interpPosGoaToNetCDFtimes.e \
    | grep "=> /" \
    | grep -v "libc\.so\|libm\.so\|libpthread\.so\|libdl\.so\|librt\.so\|linux-vdso" \
    | awk '{print $3}' \
    | xargs -I{} cp {} /tmp/orbit_libs/

FROM ${BASE_IMAGE}

COPY pipeline/daily_file_gen/daily_files/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy large files from the intermediate stage
COPY --from=intermediate /tmp/ref_files ${LAMBDA_TASK_ROOT}/daily_files/ref_files

# Copy the compiled orbit-swap binary and its bundled shared libraries
RUN mkdir -p ${LAMBDA_TASK_ROOT}/bin ${LAMBDA_TASK_ROOT}/libs
COPY --from=c_builder /tmp/interpPosGoaToNetCDF/interpPosGoaToNetCDFtimes.e ${LAMBDA_TASK_ROOT}/bin/
COPY --from=c_builder /tmp/orbit_libs/ ${LAMBDA_TASK_ROOT}/libs/

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

# Copy other files from the local directory
COPY pipeline/daily_file_gen/daily_files/daily_files daily_files/
COPY pipeline/daily_file_gen/daily_files/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/daily_files"
ENV LD_LIBRARY_PATH="${LAMBDA_TASK_ROOT}/libs"

CMD [ "app.handler" ]