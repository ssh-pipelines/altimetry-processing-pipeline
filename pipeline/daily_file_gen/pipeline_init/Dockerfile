FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/infra/pipeline_init/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pipeline/infra/pipeline_init/app.py .
COPY pipeline/infra/pipeline_init/config/ config/

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
