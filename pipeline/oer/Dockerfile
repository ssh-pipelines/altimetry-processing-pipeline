FROM public.ecr.aws/lambda/python:3.11

COPY pipeline/oer/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/oer/oer oer/
COPY pipeline/oer/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/oer"

CMD [ "app.handler" ]