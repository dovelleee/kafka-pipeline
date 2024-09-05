FROM python:3.9

WORKDIR /app

# copying the development version of the kafka_client package
COPY ./kafka_client ./kafka_client
COPY ./stage_three .
COPY ./schema ./schema

RUN cd ./kafka_client && pip install . && cd ..

ENTRYPOINT [ "python3", "app.py" ]
