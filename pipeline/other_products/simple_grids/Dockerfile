ARG BASE_REGISTRY
ARG BASE_TAG
FROM ${BASE_REGISTRY}/pipeline_runtime:${BASE_TAG}

COPY pipeline/other_products/simple_grids/requirements.txt .
RUN yum install gcc g++ gcc-c++ -y && yum -y clean all
RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt

WORKDIR ${LAMBDA_TASK_ROOT}

COPY utilities/ utilities/
COPY setup.py setup.py

RUN python -m pip install .

COPY pipeline/other_products/simple_grids/simple_gridder simple_gridder/
COPY pipeline/other_products/simple_grids/app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/simple_gridder"

CMD [ "app.handler" ]