ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY pipeline/daily_file_gen/xover/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/daily_file_gen/xover/crossover crossover/
COPY pipeline/daily_file_gen/xover/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/crossover"

CMD [ "app.handler" ]
