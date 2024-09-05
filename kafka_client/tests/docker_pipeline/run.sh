#!/bin/bash
rsync -a ../../../kafka_client .
docker compose up
