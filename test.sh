#!/bin/sh

docker build -t stream-analysis .
docker run -v test:/app/test -v logs:/app/logs stream-analysis
