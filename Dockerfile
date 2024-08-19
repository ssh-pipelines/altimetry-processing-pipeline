FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy other files from the local directory
COPY ./oer ${LAMBDA_TASK_ROOT}/oer
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/oer"

CMD [ "app.handler" ]