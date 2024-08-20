FROM public.ecr.aws/lambda/python:3.11

COPY ./finalizer/requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./utilites ${LAMBDA_TASK_ROOT}/utilities
RUN pip install -e ${LAMBDA_TASK_ROOT}/utilities

COPY ./finalizer/finalization ${LAMBDA_TASK_ROOT}/finalization
COPY ./finalizer/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/finalization"

CMD [ "app.handler" ]
