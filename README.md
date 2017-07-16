# kafka-connect-fluentd

## Running in development

Build jar:

```
$ ./gradlew shadowJar
```

Set up Apache Kafka.

```
$ tar xf kafka_2.11-0.11.0.0.tgz
$ cd kafka_2.11-0.11.0.0
```

Run Apache Kafka:

```
(on terminal 1)
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties 
(on terminal 2)
$ ./bin/kafka-server-start.sh config/server.properties
```

Run kafka-connect-fluentd (FluentdSourceConnector/FluentdSinkConnector):

```
(on terminal 3)
$ bin/connect-standalone.sh config/connect-standalone.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSourceConnector.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSinkConnector.properties
```

**:NOTE:**

Copy jar file to `CLASSPATH` or change `plugin.path` in connect-standalone.properties.
Use same `topics` in FluentdSourceConnector.properties and connect-file-sink.properties.

And emit records:

```
(on terminal 4)
$ echo '{"messmages": "Hi, Kafka connect!"}' | fluent-cat connect-test --time-as-integer
```

**NOTE:**

Specify tag same as topics in FluentdSourceConnector.properties and FluentdSinkConnector.properties.

