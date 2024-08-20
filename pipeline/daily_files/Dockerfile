FROM alpine AS intermediate

COPY .daily_files/daily_files/ref_files /tmp/ref_files

FROM public.ecr.aws/lambda/python:3.11

COPY .daily_files/requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# Copy large files from the intermediate stage
COPY --from=intermediate /tmp/ref_files ${LAMBDA_TASK_ROOT}/daily_files/ref_files

COPY ./utilites ${LAMBDA_TASK_ROOT}/utilities
RUN pip install -e ${LAMBDA_TASK_ROOT}/utilities

# Copy other files from the local directory
COPY .daily_files/daily_files ${LAMBDA_TASK_ROOT}/daily_files
COPY .daily_files/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/daily_files"

CMD [ "app.handler" ]