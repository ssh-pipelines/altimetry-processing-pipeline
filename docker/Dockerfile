#build command: docker build -t crossovers-app .
#run command: docker run -it --rm --name my-running-app -v $(pwd):/usr/src/app crossovers-app
FROM python:3.9

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the requirements.txt file into the container at /usr/src/app
COPY requirements.txt ./

# Upgrade pip and install required packages from requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt

# The default command to run when the container starts
CMD ["python", "./find_crossovers.py"]
