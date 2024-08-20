FROM public.ecr.aws/lambda/python:3.11

COPY ./bad_pass/requirements.txt .

RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./utilities ${LAMBDA_TASK_ROOT}/utilities
RUN pip install -e ${LAMBDA_TASK_ROOT}/utilities

COPY ./bad_pass/bad_passes ${LAMBDA_TASK_ROOT}/bad_passes
COPY ./bad_pass/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/bad_passes"

CMD [ "app.handler" ]
