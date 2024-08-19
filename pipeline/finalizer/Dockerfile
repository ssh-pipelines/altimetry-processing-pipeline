FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./finalization ${LAMBDA_TASK_ROOT}/finalization
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/finalization"

CMD [ "app.handler" ]
