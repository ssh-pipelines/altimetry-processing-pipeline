FROM public.ecr.aws/lambda/python:3.11

COPY ./oer/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY ./utilites ${LAMBDA_TASK_ROOT}/utilities
RUN pip install -e ${LAMBDA_TASK_ROOT}/utilities

# Copy other files from the local directory
COPY ./oer/oer ${LAMBDA_TASK_ROOT}/oer
COPY ./oer/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/oer"

CMD [ "app.handler" ]