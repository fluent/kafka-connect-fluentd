# kafka-connect-fluentd

## Running in development

Build jar:

```
$ ./gradlew jar
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

**NOTE:**

Copy jar file to `CLASSPATH` or change `plugin.path` in connect-standalone.properties.
Use same `topics` in FluentdSourceConnector.properties and FluentdSinkConnector.properties.

And emit records:

```
(on terminal 4)
$ echo '{"messmages": "Hi, Kafka connect!"}' | fluent-cat fluentd-test --time-as-integer
```

**NOTE:**

Specify tag same as topics in FluentdSourceConnector.properties and FluentdSinkConnector.properties.

### Configuration for FluentdSourceConnector

* fluentd.port
  * Port number to listen. Default: `24224`
* fluentd.bind
  * Bind address to listen. Default: `0.0.0.0`
* fluentd.chunk.size.limit
  * Allowable chunk size. Default: `Long.MAX_VALUE`
* fluentd.backlog
  * The maximum number of pending connections for a server. Default: `0`
* fluentd.send.buffer.bytes
  * `SO_SNDBUF` for forward connection. `0` means system default value. Default: `0`
* fluentd.receve.buffer.bytes
  * `SO_RCVBUF` for forward connection. `0` means system default value. Default: `0`
* fluentd.keep.alive.enabled
  * If `true`, `SO_KEEPALIVE` is enabled. Default: `true`
* fluentd.tcp.no.delay.enabled
  * If `true`, `TCP_NODELAY` is enabled. Default: `true`
* fluentd.worker.pool.size
  * Event loop pool size. `0` means auto. Default: `0`
* fluentd.transport
  * Set Fluentd transport protocol to `tcp` or `tls`. Default: `tcp`
* fluentd.tls.versions
  * TLS version. `TLS`, `TLSv1`, `TLSv1.1` or `TLSv1.2`. Default: `TLSv1.2`
* fluentd.tls.ciphers
  * Cipher suites
* fluentd.keystore.path
  * Path to keystore
* fluentd.keystore.password
  * Password for keystore
* fluentd.key.password
  * Password for key
* kafka.topic
  * Topic for Kafka. `null` means using Fluentd's tag for topic dynamically. Default: `null`
* fluentd.counter.enabled
  * **For developer only** Enable counter for messages/sec. Default: `false`

### Configuration for FluentdSinkConnector

See also [Fluency](https://github.com/komamitsu/fluency).

* fluentd.connect
  * Connection specs for Fluentd. Default: localhost:24224
* fluentd.client.max.buffer.bytes
  * Max buffer size.
* fluentd.client.buffer.chunk.initial.bytes
  * Initial size of buffer chunk. Default: 1048576 (1MiB)
* fluentd.client.buffer.chunk.retention.bytes
  * Retention size of buffer chunk. Default: 4194304 (4MiB)
* fluentd.client.flush.interval
  * Buffer flush interval in msec. Default: 600(msec)
* fluentd.client.ack.response.mode
  * Enable/Disable ack response mode. Default: false
* fluentd.client.file.backup.dir
  * Enable/Disable file backup mode. Default: false
* fluentd.client.wait.until.buffer.flushed
  * Max wait until all buffers are flushed in sec. Default: 60(sec)
* fluentd.client.wait.until.flusher.terminated
  * Max wait until the flusher is terminated in sec. Default: 60(sec)
* fluentd.client.jvm.heap.buffer.mode
  * If true use JVM heap memory for buffer pool. Default: false

NOTE: Fluency doesn't support SSL/TLS yet

### Example of SSL/TLS support with Fluentd

FluentdSourceConnector.properties

```
name=FluentdSourceConnector
tasks.max=1
connector.class=org.fluentd.kafka.FluentdSourceConnector
fluentd.port=24224
fluentd.bind=0.0.0.0
fluentd.transport=tls
fluentd.keystore.path=/path/to/influent-server.jks
fluentd.keystore.password=password-for-keystore
fluentd.key.password=password-for-key
```

fluent.conf

```aconf
<source>
  @type dummy
  dummy {"message": "this is test"}
  tag test
</source>

<filter test>
  @type stdout
</filter>
<match test>
  @type forward
  transport tls
  tls_cert_path /path/to/ca_cert.pem
  # tls_verify_hostname false # for test
  heartbeat_type none
  <server>
    # first server
    host 127.0.0.1
    port 24224
  </server>
  <buffer>
    flush_interval 1
  </buffer>
</match>
```

Run kafka-connect-fluentd and then run Fluentd with above configuration:

```text
(on terminal 1)
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
(on terminal 2)
$ ./bin/kafka-server-start.sh config/server.properties
(on terminal 3)
$ bin/connect-standalone.sh config/connect-standalone.properties \
    /path/to/kafka-connect-fluentd/config/FluentdSourceConnector.properties \
    /path/to/connect-file-sink.properties
(on terminal 4)
$ fluentd -c fluent.conf
```

