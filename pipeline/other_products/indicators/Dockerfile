ARG BASE_IMAGE=public.ecr.aws/lambda/python:3.11
FROM ${BASE_IMAGE}

COPY pipeline/other_products/indicators/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/other_products/indicators/ref_files ref_files/
COPY pipeline/other_products/indicators/indicators indicators/
COPY pipeline/other_products/indicators/app.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}/indicators"

CMD [ "app.handler" ]
