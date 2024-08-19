FROM alpine AS intermediate

COPY ./daily_files/ref_files /tmp/ref_files

FROM public.ecr.aws/lambda/python:3.10

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy large files from the intermediate stage
COPY --from=intermediate /tmp/ref_files ${LAMBDA_TASK_ROOT}/daily_files/ref_files

# Copy other files from the local directory
COPY ./daily_files ${LAMBDA_TASK_ROOT}/daily_files
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/daily_files"

CMD [ "app.handler" ]