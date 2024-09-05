#!/bin/bash

docker build -t 4choob.local:5001/etl-kafka/test-s1 -f ./stage_one/Dockerfile .
docker push 4choob.local:5001/etl-kafka/test-s1
