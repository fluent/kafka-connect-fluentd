/**
 * Copyright 2017 ClearCode Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.fluentd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;


public class FluentdSourceConnectorConfig extends AbstractConfig {

    public static final String FLUENTD_PORT = "fluentd.port";
    public static final String FLUENTD_BIND = "fluentd.bind";
    public static final String FLUENTD_CHUNK_SIZE_LIMIT = "fluentd.chunk.size.limit";
    public static final String FLUENTD_BACKLOG = "fluentd.backlog";
    public static final String FLUENTD_SEND_BUFFER_BYTES = "fluentd.send.buffer.bytes";
    public static final String FLUENTD_RECEVE_BUFFER_BYTES = "fluentd.receve.buffer.bytes";
    public static final String FLUENTD_KEEP_ALIVE_ENABLED = "fluentd.keep.alive.enabled";
    public static final String FLUENTD_TCP_NO_DELAY_ENABLED = "fluentd.tcp.no.delay.enabled";
    public static final String FLUENTD_WORKER_POOL_SIZE = "fluentd.worker.pool.size";
    public static final String FLUENTD_TRANSPORT = "fluentd.transport";
    public static final String FLUENTD_TLS_VERSIONS = "fluentd.tls.versions";
    public static final String FLUENTD_TLS_CIPHERS = "fluentd.tls.ciphers";
    public static final String FLUENTD_KEYSTORE_PATH = "fluentd.keystore.path";
    public static final String FLUENTD_KEYSTORE_PASSWORD = "fluentd.keystore.password";
    public static final String FLUENTD_KEY_PASSWORD = "fluentd.key.password";

    public static final String KAFKA_TOPIC = "kafka.topic";
    public static final String FLUENTD_SCHEMAS_ENABLE = "fluentd.schemas.enable";
    public static final String FLUENTD_COUNTER_ENABLED = "fluentd.counter.enabled";

    public FluentdSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FluentdSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FLUENTD_PORT, Type.INT, 24224, Importance.HIGH,
                        "Port number to listen. Default: 24224")
                .define(FLUENTD_BIND, Type.STRING, "0.0.0.0", Importance.HIGH,
                        "Bind address to listen. Default: 0.0.0.0")
                .define(FLUENTD_CHUNK_SIZE_LIMIT, Type.LONG, Long.MAX_VALUE, Importance.MEDIUM,
                        "Allowable chunk size. Default: Long.MAX_VALUE")
                .define(FLUENTD_BACKLOG, Type.INT, 0, Importance.MEDIUM,
                        "The maximum number of pending connections for a server. Default: 0")
                .define(FLUENTD_SEND_BUFFER_BYTES, Type.INT, 0, Importance.MEDIUM,
                        "SO_SNDBUF for forward connection. 0 means system default value. Default: 0")
                .define(FLUENTD_RECEVE_BUFFER_BYTES, Type.INT, 0, Importance.MEDIUM,
                        "SO_RCVBUF for forward connection. 0 means system default value. Default: 0")
                .define(FLUENTD_KEEP_ALIVE_ENABLED, Type.BOOLEAN, true, Importance.MEDIUM,
                        "If true SO_KEEPALIVE is enabled. Default: true")
                .define(FLUENTD_TCP_NO_DELAY_ENABLED, Type.BOOLEAN, true, Importance.MEDIUM,
                        "If true TCP_NODELAY is enabled. Default: true")
                .define(FLUENTD_WORKER_POOL_SIZE, Type.INT, 0, Importance.MEDIUM,
                        "Event loop pool size. 0 means auto. Default: 0")
                .define(FLUENTD_TRANSPORT, Type.STRING, "tcp", Importance.MEDIUM, "tcp or tls")
                .define(FLUENTD_TLS_VERSIONS, Type.LIST, "TLSv1.2", Importance.MEDIUM,
                        "TLS version. \"TLS\", \"TLSv1\", \"TLSv1.1\" or \"TLSv1.2\". Default: TLSv1.2")
                .define(FLUENTD_TLS_CIPHERS, Type.LIST, null, Importance.MEDIUM, "Cipher suites")
                .define(FLUENTD_KEYSTORE_PATH, Type.STRING, null, Importance.MEDIUM,
                        "Path to keystore")
                .define(FLUENTD_KEYSTORE_PASSWORD, Type.STRING, null, Importance.MEDIUM,
                        "Password for keystore")
                .define(FLUENTD_KEY_PASSWORD, Type.STRING, null, Importance.MEDIUM,
                        "Password for key")
                .define(KAFKA_TOPIC, Type.STRING, null, Importance.MEDIUM,
                        "Topic for Kafka. null means using Fluentd's tag for topic dynamically. Default: null")
                .define(FLUENTD_SCHEMAS_ENABLE, Type.BOOLEAN, true, Importance.MEDIUM,
                        "Enable schemas for messages. Default: true")
                .define(FLUENTD_COUNTER_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM,
                        "Enable counter for messages/sec. Default: false");
    }

    public int getFluentdPort() {
        return getInt(FLUENTD_PORT);
    }

    public String getFluentdBind() {
        return getString(FLUENTD_BIND);
    }

    public SocketAddress getLocalAddress() throws FluentdConnectorConfigError {
        try {
            return new InetSocketAddress(InetAddress.getByName(getFluentdBind()), getFluentdPort());
        } catch (UnknownHostException ex) {
            throw new FluentdConnectorConfigError(ex.getMessage());
        }
    }

    public Long getFluentdChunkSizeLimit() {
        return getLong(FLUENTD_CHUNK_SIZE_LIMIT);
    }

    public int getFluentdBacklog() {
        return getInt(FLUENTD_BACKLOG);
    }

    public int getFluentdSendBufferSize() {
        return getInt(FLUENTD_SEND_BUFFER_BYTES);
    }

    public int getFluentdReceveBufferSize() {
        return getInt(FLUENTD_RECEVE_BUFFER_BYTES);
    }

    public boolean getFluentdKeepAliveEnabled() {
        return getBoolean(FLUENTD_KEEP_ALIVE_ENABLED);
    }

    public boolean getFluentdTcpNoDeleyEnabled() {
        return getBoolean(FLUENTD_TCP_NO_DELAY_ENABLED);
    }

    public int getFluentdWorkerPoolSize() {
        return getInt(FLUENTD_WORKER_POOL_SIZE);
    }

    public String getFluentdTransport() {
        return getString(FLUENTD_TRANSPORT);
    }

    public List<String> getFluentdTlsVersions() {
        return getList(FLUENTD_TLS_VERSIONS);
    }

    public String getFluentdKeystorePath() {
        return getString(FLUENTD_KEYSTORE_PATH);
    }

    public String getFluentdKeystorePassword() {
        return getString(FLUENTD_KEYSTORE_PASSWORD);
    }

    public String getFluentdKeyPassword() {
        return getString(FLUENTD_KEY_PASSWORD);
    }

    public String getFluentdStaticTopic() {
        return getString(KAFKA_TOPIC);
    }

    public boolean isFluentdSchemasEnable() {
        return getBoolean(FLUENTD_SCHEMAS_ENABLE);
    }

    public boolean getFluentdCounterEnabled() {
        return getBoolean(FLUENTD_COUNTER_ENABLED);
    }
}
