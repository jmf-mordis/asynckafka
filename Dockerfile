FROM python:3.8

WORKDIR /opt/asynckafka

COPY . .

RUN apt-get update && \
    apt-get install -y librdkafka1 librdkafka-dev && \
    pip install -r requirements.txt

CMD make compile && make test