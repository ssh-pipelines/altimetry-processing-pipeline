# Build context must be root nasa-ssh-pipeline directory

FROM alpine AS intermediate

COPY pipeline/daily_files/daily_files/ref_files /tmp/ref_files

FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/daily_files/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy large files from the intermediate stage
COPY --from=intermediate /tmp/ref_files ${LAMBDA_TASK_ROOT}/daily_files/ref_files

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

# Copy other files from the local directory
COPY pipeline/daily_files/daily_files daily_files/
COPY pipeline/daily_files/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/daily_files"

CMD [ "app.handler" ]