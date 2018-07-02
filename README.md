# kafka-consumer-alpakka
Testing both akka &amp; kafka through alpakka

Tests are issued with confluent open source kafka distro, found here
https://www.confluent.io/product/confluent-open-source/

Kafka is expected listening at localhost:9092
Test topic is named "topic1" (tada!)

Development done with Kafka 1.0.1

## Actors

```
+----------------+     +-------------+      +--------------+
| ReflexConsumer | --> | FilterActor | -->  | DisplayActor |
+----------------+     +-------------+      +--------------+
       |                      |
       |                      | if too long
       |                      v
       |               +-------------+
    if +-------------> | RejectActor |
    badly encoded      +-------------+
```

## Testing

```
kafka-console-producer --broker-list localhost:9092 --topic topic1 --property "parse.key=true" --property "key.separator=:"
```
Valid messages are Json messages with a single string attribute named "text".
Messages with text longer than 10 chars are filtered out by the filter actor functor.

Passing message
```
456:{"text":"Message"}
```
Filtered message
```
123:{"text":"sdfkjqsklmgfqsrhjzrhjkqshljkfhsjklqfhjklqzherljkhzel"}
```
Spurious messages
```
789:{"text":123}
890:Hello world!
```
