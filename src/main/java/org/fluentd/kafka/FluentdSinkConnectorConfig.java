package org.fluentd.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class FluentdSinkConnectorConfig extends AbstractConfig {

    static final String FLUENTD_CONNECT = "fluentd.connect";
    static final String FLUENTD_CLIENT_TCP_HEART_BEAT = "fluentd.client.tcp.heart.beat";
    static final String FLUENTD_CLIENT_REQUIRE_ACK_RESPONSE = "fluentd.client.require.ack.response";
    static final String FLUENTD_CLIENT_FLUSH_INTERVAL = "fluentd.client.flush.interval";
    static final String FLUENTD_CLIENT_BUFFER_CHUNK_INITIAL = "fluentd.client.buffer.chunk.initial.bytes";
    static final String FLUENTD_CLIENT_BUFFER_CHUNK_RETENTION = "fluentd.client.buffer.chunk.retention.bytes";
    static final String FLUENTD_CLIENT_BUFFER_MAX = "fluentd.client.buffer.max.bytes";
    // static final String FLUENTD_CLIENT_


    public FluentdSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FluentdSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FLUENTD_CONNECT, Type.STRING, "localhost:24224", Importance.HIGH, "Connection specs for Fluentd");
    }

    public String getFluentdConnect() {
        return getString(FLUENTD_CONNECT);
    }

    public List<InetSocketAddress> getFluentdConnectAddresses() {
        List<InetSocketAddress> addresses = new ArrayList<>();
        for (String address : getString(FLUENTD_CONNECT).split(",")) {
            String[] parsed = address.split(":");
            String host = parsed[0];
            int port = new Integer(parsed[1]);
            addresses.add(new InetSocketAddress(host, port));
        }
        return addresses;
    }

}
