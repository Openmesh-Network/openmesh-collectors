# syntax=docker/dockerfile:1

FROM python:3.10.4-slim
WORKDIR /src
COPY l3_atom/ ./l3_atom
COPY runner.py ./
RUN apt-get update
RUN apt-get install -y --no-install-recommends gcc musl-dev librdkafka-dev build-essential
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY config.ini ./