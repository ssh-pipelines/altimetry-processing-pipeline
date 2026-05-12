ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY pipeline/daily_file_gen/finalizer/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/daily_file_gen/finalizer/finalization finalization/
COPY pipeline/daily_file_gen/finalizer/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/finalization"

CMD [ "app.handler" ]
