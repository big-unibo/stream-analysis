# Start API
From project root:
- `docker build -t stream-generator -f delivery/DockerfileGenerator .` 
- `docker build -t stream-algorithm -f delivery/DockerfileConsumer .` 

Run containers:
- `docker run -p 8080:8080 stream-generator`
- `docker run -p 8084:8084 stream-algorithm`

Containers in cluster registry:
```shell
docker image build --tag 127.0.0.0:5000/stream_analysis_consumer:v1.0.0 -f DockerfileConsumer .
docker image build --tag 127.0.0.0:5000/stream_analysis_generator:v1.0.0 -f DockerfileGenerator .
docker push 127.0.0.0:5000/stream_analysis_consumer:v1.0.0
docker push 127.0.0.0:5000/stream_analysis_generator:v1.0.0
```