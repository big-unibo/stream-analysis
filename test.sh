#!/bin/bash

# Build the Docker image with the tag 'stream-analysis'
docker build -t stream-analysis .

# Create necessary directories if they don't exist
mkdir -p "$(pwd)/test"
mkdir -p "$(pwd)/test/graphs"
mkdir -p "$(pwd)/test/tables"
mkdir -p "$(pwd)/logs"

# Run the Docker container, mounting the test and logs directories
docker run -v "$(pwd)/test:/app/test" -v "$(pwd)/logs:/app/logs" stream-analysis
