# Spark Streaming to evaluate and update our model

## Producer

First, you have to start Kafka and ZooKeeper servers:

```bash
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

Then, create a topic named "bikes":
```bash
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bikes
```

And to run the producer:
```bash
sbt run
```

You can consume some messages to see the output:
```bash
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic avg --from-beginning
```
