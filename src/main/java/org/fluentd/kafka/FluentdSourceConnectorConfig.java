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
    // public static final String FLUENTD_CHUNK_SIZE_LIMIT = "fluentd.chunk.size.limit";
    // public static final String FLUENTD_WORKER_POOL_SIZE = "fluentd.worker.pool.size";

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
                        "Bind address to listen. Default: 0.0.0.0");
    }

    public int getFluentdPort() {
        return this.getInt(FLUENTD_PORT);
    }

    public String getFluentdBind() {
        return this.getString(FLUENTD_BIND);
    }

    public SocketAddress getLocalAddress() throws FluentdConnectorConfigError {
        try {
            return new InetSocketAddress(InetAddress.getByName(getFluentdBind()), getFluentdPort());
        } catch (UnknownHostException ex) {
            throw new FluentdConnectorConfigError(ex.getMessage());
        }
    }
}
