FROM python:3.7 AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install kafka-python

ENV HOME /app
WORKDIR $HOME
