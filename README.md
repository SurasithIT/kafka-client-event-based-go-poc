# Kafka Client Golang Event-based POC
Kafka client for event-based concept. This example, will have 2 topics, one is **alarm-topic** is use for *trigger event*, another is **info-topic** is use as the *queue* of messages. 

If consumer received the message from *alarm-topic* will poll all messages from *info-topic* to do something after.

## Sequence Diagram

### Producer
```mermaid
%% Sequence diagram of producers
  sequenceDiagram
    Producer->>Kafka Broker: produce message to alarm-topic<br/> (for example will produce every 20 seconds)
    Producer->>Kafka Broker: produce message to info-topic<br/> (for example will produce every 3 seconds)
```

### Consumer
```mermaid
%% Sequence diagram of consumers
  sequenceDiagram
    Kafka Broker->>Consumer: read message from alarm-topic
    alt have alarm message
    Kafka Broker->>Consumer: poll all uncommitted messages from alarm-topic
    Consumer->>+Consumer: process message
    end
```