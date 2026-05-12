ARG BASE_REGISTRY
ARG BASE_TAG
FROM ${BASE_REGISTRY}/pipeline_runtime:${BASE_TAG}

COPY pipeline/daily_file_gen/bad_pass/requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/daily_file_gen/bad_pass/bad_passes bad_passes/
COPY pipeline/daily_file_gen/bad_pass/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/bad_passes"

CMD [ "app.handler" ]
