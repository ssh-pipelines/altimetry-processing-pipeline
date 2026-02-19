FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/daily_file_gen/unifier/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY pipeline/daily_file_gen/unifier/app.py .
COPY pipeline/daily_file_gen/unifier/config/ config/

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
