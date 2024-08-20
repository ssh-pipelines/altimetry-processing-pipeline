FROM public.ecr.aws/lambda/python:3.11

COPY ./xover/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY ./utilites ${LAMBDA_TASK_ROOT}/utilities
RUN pip install -e ${LAMBDA_TASK_ROOT}/utilities

COPY ./xover/crossover ${LAMBDA_TASK_ROOT}/crossover
COPY ./xover/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/crossover"

CMD [ "app.handler" ]
