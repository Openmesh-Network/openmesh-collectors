# syntax=docker/dockerfile:1

FROM python:3.10.4-alpine3.15
WORKDIR /src
COPY src/ .
RUN apk add --no-cache gcc musl-dev librdkafka-dev
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY config.ini ./
ENTRYPOINT ["python3"]
CMD ["bybit.py"]