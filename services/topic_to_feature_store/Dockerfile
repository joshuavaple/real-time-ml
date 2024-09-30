# let's start with a slim python image for 3.12:
FROM python:3.12-slim

# set the working directory in the container
WORKDIR /app

# Install build-essentials, includinng gcc and other tools
RUN apt-get update && apt-get install -y build-essential

# isntall Python poetry with version 1.8.3
RUN pip install poetry==1.8.3

# copy the source code to the working directory
COPY . .

# install dependencies
RUN poetry install

# command to run on container start
CMD [ "poetry", "run", "python", "src/main.py" ]