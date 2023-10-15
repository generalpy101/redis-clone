FROM python:3.9-bullseye

# Update packages
RUN apt-get update
RUN apt-get upgrade -y

# Allow for logging to be printed to the terminal instead of buffered
ENV PYTHONUNBUFFERED 1

# Make a directory for the app
RUN mkdir -p /usr/src/redis-clone

# Set the working directory
WORKDIR /usr/src/redis-clone

# Copy required files
COPY redis_clone /usr/src/redis-clone/redis_clone
COPY requirements-dev.txt /usr/src/redis-clone/
COPY setup.py /usr/src/redis-clone/
COPY tests /usr/src/redis-clone/tests
COPY README.md /usr/src/redis-clone/

# Install dependencies
RUN pip install -r requirements-dev.txt

# Install the app currently as dev always
RUN pip install -e ".[dev]"

# Run bash
CMD ["/bin/bash"]
