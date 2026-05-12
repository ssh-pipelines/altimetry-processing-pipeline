FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/pipeline_runtime/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt
