# syntax=docker/dockerfile:1

FROM python:3.11.0a7-alpine3.15
WORKDIR /src
RUN apk add --no-cache gcc musl-dev librdkafka-dev
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY src/helpers ./helpers
COPY src/sink_connector ./sink_connector
COPY src/orderbooks ./orderbooks
COPY src/normalise/bybit_normalisation.py src/normalise/__init__.py ./normalise/
COPY src/__init__.py src/bybit.py ./
COPY ca-aiven-cert.pem jay.cert jay.key config.ini ./
CMD ["python3", "bybit.py"]