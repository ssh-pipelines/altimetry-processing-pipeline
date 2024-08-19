#build command: docker build -t crossovers-app .
#run command: docker run -it --rm --name my-running-app -v $(pwd):/usr/src/app crossovers-app
FROM public.ecr.aws/lambda/python:3.11

# Copy the requirements.txt file into the container at /usr/src/app
COPY requirements.txt .

# Upgrade pip and install required packages from requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ./crossover ${LAMBDA_TASK_ROOT}/crossover
COPY app.py .

ENV PYTHONPATH="${PYTHONPATH}:${LAMBDA_TASK_ROOT}/crossover"

# The default command to run when the container starts
CMD [ "app.handler" ]
