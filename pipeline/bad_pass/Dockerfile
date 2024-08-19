FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./bad_passes ${LAMBDA_TASK_ROOT}/bad_passes
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/bad_passes"

CMD [ "app.handler" ]
