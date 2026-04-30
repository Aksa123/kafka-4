Basic
1. create .env file from .env-sample
2. run docker compose
    docker compose up

Notes:
    - A cronjob will run every 20s to:
        1. check-new-topics: periodically check for new topics. If found, sends trigger check-new-topics to consumer
        2. flush: flush messages in the internal queue (buffer)

Create a new topic
    python3 code/admin.py --create-topic mytopic --partitions 2

Get topic list
    python3 code/admin.py --list-topic

Publish a message
    python3 code/producer.py --topic mytopic --msg "hello world" --part 1

Access broker shell script
    docker exec --workdir /opt/kafka/bin/ -it kafka-broker sh


Notes:
- Currently confluent_kafka lib is used instead of kafka-python
- All consumer nodes shall consume ALL topics but different partitions. Scalable??