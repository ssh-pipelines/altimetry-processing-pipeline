FROM public.ecr.aws/lambda/python:3.11

ARG ROOT_DIR=pipeline/other_products/enso
ARG CARTOPY_DATA_DIR=/opt/cartopy_data

ENV CARTOPY_OFFLINE=true
ENV CARTOPY_DATA_DIR=$CARTOPY_DATA_DIR

# Copy and install requirements
COPY $ROOT_DIR/requirements.txt .

RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt

# Create cartopy data directory and set permissions
RUN mkdir -p $CARTOPY_DATA_DIR && chmod -R a+rwx $CARTOPY_DATA_DIR

# Copy setup_cartopy.py into the container
COPY $ROOT_DIR/cartopy_setup.py .

# Run the setup_cartopy.py script
RUN python cartopy_setup.py

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY $ROOT_DIR/. ${LAMBDA_TASK_ROOT}

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/enso"

CMD [ "app.handler" ]