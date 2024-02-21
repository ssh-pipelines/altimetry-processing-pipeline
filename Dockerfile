FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt .

RUN pip install --upgrade pip \ 
    && pip install --no-cache-dir -r requirements.txt

COPY ./daily_files ${LAMBDA_TASK_ROOT}/daily_files
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/daily_files"

CMD [ "app.handler" ]