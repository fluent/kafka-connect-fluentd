package org.fluentd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Map;


public class FluentdSourceConnectorConfig extends AbstractConfig {

    /*
     * in_forward configs
     *   port
     *   bind
     *   linger_timeout
     *   check_size_limit
     *   chunk_size_warn_limit
     *   skip_invalid_event
     *   source_hostname_key
     *   log_level
     */
    /*
     * influent config
     *   localAddress
     *   chunkSizeLimit
     *   sendBufferSize
     *   receiveBufferSize
     *   keepAliveEnabled
     *   tcpNoDelayEnabled
     *   workerPoolSize
     */
    public static final String FLUENTD_PORT = "fluentd.port";
    public static final String FLUENTD_BIND = "fluentd.bind";
    public static final String FLUENTD_CHUNK_SIZE_LIMIT = "fluentd.chunk.size.limit";
    public static final String FLUENTD_BACKLOG = "fluentd.backlog";
    public static final String FLUENTD_SEND_BUFFER_SIZE = "fluentd.send.buffer.size";
    public static final String FLUENTD_RECEIVE_BUFFER_SIZE = "fluentd.receve.buffer.size";
    public static final String FLUENTD_KEEP_ALIVE_ENABLED = "fluentd.keep.alive.enabled";
    public static final String FLUENTD_TCP_NO_DELAY_ENABLED = "fluentd.tcp.no.delay.enabled";
    public static final String FLUENTD_WORKER_POOL_SIZE = "fluentd.worker.pool.size";
    public static final String FLUENTD_PROTOCOL = "fluentd.protocol";
    public static final String FLUENTD_TLS_VERSION = "fluentd.tls.version";
    public static final String FLUENTD_KEYSTORE_PATH = "fluentd.keystore.path";
    public static final String FLUENTD_KEYSTORE_PASSWORD = "fluentd.keystore.password";
    public static final String FLUENTD_KEY_PASSWORD = "fluentd.key.password";
    public static final String FLUENTD_TRUSTSTORE_PATH = "fluentd.truststore.path";
    public static final String FLUENTD_TRUSTSTORE_PASSWORD = "fluentd.truststore.password";

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
                .define(FLUENTD_SEND_BUFFER_SIZE, Type.INT, 0, Importance.MEDIUM,
                        "SO_SNDBUF for forward connection. 0 means system default value. Default: 0")
                .define(FLUENTD_RECEIVE_BUFFER_SIZE, Type.INT, 0, Importance.MEDIUM,
                        "SO_RCVBUF for forward connection. 0 means system default value. Default: 0")
                .define(FLUENTD_KEEP_ALIVE_ENABLED, Type.BOOLEAN, true, Importance.MEDIUM,
                        "If true SO_KEEPALIVE is enabled. Default: true")
                .define(FLUENTD_TCP_NO_DELAY_ENABLED, Type.BOOLEAN, true, Importance.MEDIUM,
                        "If true TCP_NODELAY is enabled. Default: true")
                .define(FLUENTD_WORKER_POOL_SIZE, Type.INT, 0, Importance.MEDIUM,
                        "Event loop pool size. 0 means auto. Default: 0")
                .define(FLUENTD_PROTOCOL, Type.STRING, "TCP", Importance.MEDIUM,
                        "Protocol type name. TCP or TLS. Default: TCP")
                .define(FLUENTD_TLS_VERSION, Type.STRING, "TLS", Importance.MEDIUM,
                        "TLS version. \"TLS\", \"TLSv1\", \"TLSv1.1\" or \"TLSv1.2\". Default: TLS")
                .define(FLUENTD_KEYSTORE_PATH, Type.STRING, null, Importance.MEDIUM,
                        "Path to keystore")
                .define(FLUENTD_KEYSTORE_PASSWORD, Type.STRING, null, Importance.MEDIUM,
                        "Password for keystore")
                .define(FLUENTD_KEY_PASSWORD, Type.STRING, null, Importance.MEDIUM,
                        "Password for key")
                .define(FLUENTD_TRUSTSTORE_PATH, Type.STRING, null, Importance.MEDIUM,
                        "Path to truststore")
                .define(FLUENTD_TRUSTSTORE_PASSWORD, Type.STRING, null, Importance.MEDIUM,
                        "Password for truststore");
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
        return getInt(FLUENTD_SEND_BUFFER_SIZE);
    }

    public int getFluentdReceveBufferSize() {
        return getInt(FLUENTD_RECEIVE_BUFFER_SIZE);
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

    public String getFluentdProtocol() {
        return getString(FLUENTD_PROTOCOL);
    }

    public String getFluentdTlsVersion() {
        return getString(FLUENTD_TLS_VERSION);
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

    public String getFluentdTruststorePath() {
        return getString(FLUENTD_TRUSTSTORE_PATH);
    }

    public String getFluentdTruststorePassword() {
        return getString(FLUENTD_TRUSTSTORE_PASSWORD);
    }
}
