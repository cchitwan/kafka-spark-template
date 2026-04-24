# Modernization Guide

This repository was modernized to use:

- Java 11
- Apache Spark 3.5.x (Structured Streaming)
- Scala 2.12
- Kafka clients 3.x

What changed

- Replaced DStream-based Kafka consumer with Structured Streaming (spark-sql-kafka-0-10 connector) in BasicKafkaSingleStreamProcessor.
- Updated IConfig to include `checkpointLocation` and `startingOffsets` for Structured Streaming.
- Switched logging to SLF4J + Logback; added `logback.xml` default config under resources.
- Improved PhoenixConnectionManager with safe close and validation helpers.

How to run

1. Build with Java 11 and a Spark 3.x compatible environment:

   mvn -DskipTests package

2. Provide a config with `checkpointLocation` (required for production) and Kafka broker list (bootstrap servers). Example:

```yaml
appName: test-app
master: local[*]
kafkaStreamConfig:
  brokerList: localhost:9092
  topic: test-topic
checkpointLocation: /tmp/spark-checkpoint/test-app
startingOffsets: latest
```

3. Submit to Spark 3 cluster or run locally using `spark-submit` with the matching Spark and Scala versions.

Notes & migration tips

- Structured Streaming requires checkpointing for exactly-once semantics when using `foreachBatch`. Always set `checkpointLocation`.
- The concrete processors need to be updated: change `processAllRDDs(JavaDStream<T>)` to `processAllBatches(Dataset<T>)`. `processEachPartion(Iterator<T>)` is preserved for partition-level handling.
- Test against staging cluster; Spark 3 and Java 11 are required on the runtime.
- If you relied on Zookeeper-based offset management, switch to checkpoint-based or Kafka offset storage; remove custom offset DAOs after migration.

If you want, I can update remaining concrete processors and example apps in the repo to use the new API.
