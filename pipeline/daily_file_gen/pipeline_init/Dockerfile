FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/daily_file_gen/pipeline_init/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py .
RUN pip install .

COPY pipeline/daily_file_gen/pipeline_init/app.py .
COPY pipeline/daily_file_gen/pipeline_init/config/ config/

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
