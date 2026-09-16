# Spring Boot Kafka - One Producer + Multiple Consumers

This project is based on the single-consumer Spring Boot Kafka project you provided, but changes the architecture to **1 Producer + 3 Independent Consumers**.

## Architecture

```text
                         +----------------------+
                         |   Spring Boot        |
                         |      Producer        |
                         |      :8081           |
                         +----------+-----------+
                                    |
                                    | accountId = Kafka key
                                    v
                    +----------------------------------+
                    |       Kafka: settlement-events   |
                    |          3 Partitions             |
                    +----+---------------+--------------+
                         |               |              |
             +-----------+--+    +------+-------+    +--+----------------+
             | Settlement   |    | Audit Service |    | Notification     |
             | Consumer     |    | Consumer      |    | Consumer         |
             | :8082        |    | :8083         |    | :8084            |
             | group:       |    | group:        |    | group:           |
             | settlement-  |    | audit-group   |    | notification-    |
             | group        |    |               |    | group            |
             +--------------+    +---------------+    +------------------+

Each consumer has a DIFFERENT consumer group.
Therefore, every published message is delivered to all three applications.
```

## Projects

| Application | Port | Consumer Group | Purpose |
|---|---:|---|---|
| `kafka-producer` | 8081 | N/A | Publishes trade events |
| `kafka-settlement-consumer` | 8082 | `settlement-group` | Processes settlement |
| `kafka-audit-consumer` | 8083 | `audit-group` | Audits every trade |
| `kafka-notification-consumer` | 8084 | `notification-group` | Handles notifications |

Kafka topic: `settlement-events`

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker Desktop / Docker Engine
- Postman or curl

## 1. Start Kafka and Kafka UI

Open a terminal in the project root:

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Kafka is available to Spring Boot applications at:

```text
localhost:8085
```

Kafka UI is available at:

```text
http://localhost:8086
```

## 2. Create the Kafka Topic

The Docker configuration disables automatic topic creation, so create the topic manually.

```bash
docker exec -it kafka-sprint7 /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic settlement-events \
  --bootstrap-server localhost:29092 \
  --partitions 3 \
  --replication-factor 1
```

Verify:

```bash
docker exec -it kafka-sprint7 /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic settlement-events \
  --bootstrap-server localhost:29092
```

You should see 3 partitions.

## 3. Start the Producer

Terminal 1:

```bash
cd kafka-producer
mvn spring-boot:run
```

Producer starts on:

```text
http://localhost:8081
```

## 4. Start Consumer 1 - Settlement

Terminal 2:

```bash
cd kafka-settlement-consumer
mvn spring-boot:run
```

It uses:

```text
Group ID: settlement-group
Port: 8082
```

## 5. Start Consumer 2 - Audit

Terminal 3:

```bash
cd kafka-audit-consumer
mvn spring-boot:run
```

It uses:

```text
Group ID: audit-group
Port: 8083
```

## 6. Start Consumer 3 - Notification

Terminal 4:

```bash
cd kafka-notification-consumer
mvn spring-boot:run
```

It uses:

```text
Group ID: notification-group
Port: 8084
```

## 7. Send a Trade from Postman

Use:

```text
POST http://localhost:8081/trades?accountId=ACC-001&trade=AAPL,BUY,200
```

Expected producer output:

```text
PRODUCED -> topic=settlement-events partition=1 offset=0 key=ACC-001 value=AAPL,BUY,200
```

The exact partition and offset can differ depending on existing Kafka data and hashing.

### Settlement consumer output

```text
SETTLEMENT PROCESSOR RECEIVED MESSAGE
Topic     : settlement-events
Partition : 1
Offset    : 0
Key       : ACC-001
Value     : AAPL,BUY,200
Consumer Group: settlement-group
```

### Audit consumer output

```text
AUDIT SERVICE RECEIVED MESSAGE
Topic     : settlement-events
Partition : 1
Offset    : 0
Key       : ACC-001
Value     : AAPL,BUY,200
Consumer Group: audit-group
```

### Notification consumer output

```text
NOTIFICATION SERVICE RECEIVED MESSAGE
Topic     : settlement-events
Partition : 1
Offset    : 0
Key       : ACC-001
Value     : AAPL,BUY,200
Consumer Group: notification-group
```

## 8. Test Multiple Messages

Send several requests:

```text
POST http://localhost:8081/trades?accountId=ACC-001&trade=AAPL,BUY,200
POST http://localhost:8081/trades?accountId=ACC-002&trade=MSFT,SELL,100
POST http://localhost:8081/trades?accountId=ACC-003&trade=GOOG,BUY,50
POST http://localhost:8081/trades?accountId=ACC-004&trade=TSLA,BUY,75
```

Because the producer uses `accountId` as the Kafka key, Kafka hashes the key and selects a partition. Messages with the same key are routed to the same partition.

## Important Kafka Concept: Different Groups

This is the main difference from the original project.

```text
                    settlement-events
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
   settlement-group   audit-group   notification-group
          |                |                |
          v                v                v
      Consumer 1       Consumer 2       Consumer 3

        ALL THREE GROUPS RECEIVE THE SAME EVENT
```

A different consumer group creates an independent subscription to the topic.

### What if all consumers use the same group?

If all three applications used:

```java
@KafkaListener(topics = "settlement-events", groupId = "settlement-group")
```

then Kafka would treat them as members of the **same consumer group**.

With 3 partitions and 3 consumers, Kafka could distribute partitions approximately like:

```text
Partition 0 ---> Consumer 1
Partition 1 ---> Consumer 2
Partition 2 ---> Consumer 3
```

In that design, a single message is normally processed by only one consumer in the group.

For this project we intentionally use **three different groups**, because we want all three business services to receive every trade event.

## Kafka UI

Open:

```text
http://localhost:8086
```

Select `local-kafka` and inspect:

```text
Topics
  -> settlement-events

Consumer Groups
  -> settlement-group
  -> audit-group
  -> notification-group
```

This is a good way to demonstrate offsets and consumer groups during a class.

## Stop Everything

Stop Spring Boot applications with `Ctrl+C`.

Stop Kafka:

```bash
docker compose down
```

To also remove Kafka's stored data and start completely fresh:

```bash
docker compose down -v
```

## Teaching Flow

A simple classroom explanation:

1. **Producer** receives a REST request.
2. Producer publishes the trade to `settlement-events`.
3. Kafka stores the event in a partition.
4. `settlement-group` reads it for settlement processing.
5. `audit-group` independently reads the same event for auditing.
6. `notification-group` independently reads the same event for notification.
7. Each group maintains its own offset.

The key idea is:

> **One Kafka event can be consumed independently by multiple consumer groups.**

## Project Structure

```text
kafka-multi-consumer-project/
|
+-- docker-compose.yml
+-- README.md
|
+-- kafka-producer/
|   +-- pom.xml
|   +-- src/main/java/com/neueda/kafkaproducer/
|       +-- KafkaProducerApplication.java
|       +-- KafkaProducerService.java
|       +-- TradeController.java
|
+-- kafka-settlement-consumer/
|   +-- pom.xml
|   +-- src/main/java/com/neueda/settlementconsumer/
|       +-- SettlementConsumerApplication.java
|       +-- SettlementConsumer.java
|
+-- kafka-audit-consumer/
|   +-- pom.xml
|   +-- src/main/java/com/neueda/auditconsumer/
|       +-- AuditConsumerApplication.java
|       +-- AuditConsumer.java
|
+-- kafka-notification-consumer/
    +-- pom.xml
    +-- src/main/java/com/neueda/notificationconsumer/
        +-- NotificationConsumerApplication.java
        +-- NotificationConsumer.java
```
