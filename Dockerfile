# syntax=docker/dockerfile:1

FROM python:3.9.4-slim
WORKDIR /src
COPY openmesh/ ./openmesh
COPY static/ ./static
COPY runner.py ./
RUN apt-get update
RUN apt-get install -y --no-install-recommends gcc musl-dev librdkafka-dev build-essential
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY config.ini ./
COPY tests/ ./tests
RUN pip3 install -r tests/requirements.txt
COPY mock_data/ ./mock_data