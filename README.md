# MovieRatingConsumer

# Kafka consumer with Spark

## Installation

```bash
https://kafka.apache.org/downloads
https://www.apache.org/dyn/closer.cgi?path=/kafka/2.7.0/kafka_2.13-2.7.0.tgz

REF: https://kafka.apache.org/quickstart
```

## Starting ZooKeeper

```bash
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

## Starting Kafka Server/Broker

if we need multiple servers create a new server.properties and change broker.id and ports

```bash
./bin/kafka-server-start.sh config/server.properties
./bin/kafka-server-start.sh config/server1.properties
./bin/kafka-server-start.sh config/server2.properties
```

## Create Kafka Topic


```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic spark_kafka_poc
```

Replication factor should be less than available brokers. if we create n brokers we can have n or less replication-factor 

## To list all topics
```bash
./bin/kafka-topics.sh --list --zookeeper localhost:2181
```

## To describe topic
```bash
./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic spark_kafka_poc
```

## Produce from the Kafka Topic via Console

```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic spark_kafka_poc
```
## Consume from the Kafka Topic via Console

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic spark_kafka_poc --from-beginning
```
