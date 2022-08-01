#!/usr/bin/env bash

docker-compose up -d --wait

aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration
export S3_ENDPOINT_URL=http://localhost:4566 
pipenv run python integration_test.py
EXIT_CODE="$?"

docker-compose down

exit $EXIT_CODE