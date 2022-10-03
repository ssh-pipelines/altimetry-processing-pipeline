FROM public.ecr.aws/lambda/python:3.8

COPY requirements.txt .

RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt

COPY ./daily_files ${LAMBDA_TASK_ROOT}/daily_files
COPY app.py .

CMD [ "app.handler" ]