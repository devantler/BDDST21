docker build -t my-flume-image:latest .
docker run --rm --network confluent -ti my-flume-image bash