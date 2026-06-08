FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/infra/run_summary/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/infra/run_summary/app.py .
COPY pipeline/infra/run_summary/summarizer.py .

ENV PYTHONPATH="${LAMBDA_TASK_ROOT}"

CMD [ "app.handler" ]
