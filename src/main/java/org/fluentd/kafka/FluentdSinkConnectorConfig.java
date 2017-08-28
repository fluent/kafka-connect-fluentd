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
    static final String FLUENTD_CLIENT_MAX_BUFFER_SIZE = "fluentd.client.max.buffer.size";
    static final String FLUENTD_CLIENT_BUFFER_CHUNK_INITIAL_SIZE = "fluentd.client.buffer.chunk.initial.size";
    static final String FLUENTD_CLIENT_BUFFER_CHUNK_RETENTION_SIZE = "fluentd.client.buffer.chunk.retention.size";
    static final String FLUENTD_CLIENT_FLUSH_INTERVAL = "fluentd.client.flush.interval";
    static final String FLUENTD_CLIENT_ACK_RESPONSE_MODE = "fluentd.client.ack.response.mode";
    static final String FLUENTD_CLIENT_FILE_BACKUP_DIR = "fluentd.client.file.backup.dir";
    static final String FLUENTD_CLIENT_WAIT_UNTIL_BUFFER_FLUSHED = "fluentd.client.wait.until.buffer.flushed";
    static final String FLUENTD_CLIENT_WAIT_UNTIL_FLUSHER_TERMINATED = "fluentd.client.wait.until.flusher.terminated";
    static final String FLUENTD_CLIENT_JVM_HEAP_BUFFER_MODE = "fluentd.client.jvm.heap.buffer.mode";
    // static final String FLUENTD_CLIENT_SENDER_ERROR_HANDLER = "fluentd.client.sender.error.handler";
    // static final String FLUENTD_CLIENT_TCP_HEART_BEAT = "fluentd.client.tcp.heart.beat";

    public FluentdSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FluentdSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(FLUENTD_CONNECT, Type.STRING, "localhost:24224", Importance.HIGH,
                        "Connection specs for Fluentd")
                .define(FLUENTD_CLIENT_MAX_BUFFER_SIZE, Type.LONG, null, Importance.MEDIUM,
                        "Max buffer size.")
                .define(FLUENTD_CLIENT_BUFFER_CHUNK_INITIAL_SIZE, Type.INT, null, Importance.MEDIUM,
                        "Initial size of buffer chunk. Default: 1048576 (1MiB)")
                .define(FLUENTD_CLIENT_BUFFER_CHUNK_RETENTION_SIZE, Type.INT, null, Importance.MEDIUM,
                        "Retention size of buffer chunk. Default: 4194304 (4MiB)")
                .define(FLUENTD_CLIENT_FLUSH_INTERVAL, Type.INT, null, Importance.MEDIUM,
                        "Buffer flush interval in msec. Default: 600msec")
                .define(FLUENTD_CLIENT_ACK_RESPONSE_MODE, Type.BOOLEAN, false, Importance.MEDIUM,
                        "Enable/Disable ack response mode. Default: false")
                .define(FLUENTD_CLIENT_FILE_BACKUP_DIR, Type.STRING, null, Importance.MEDIUM,
                        "Enable/Disable file backup mode. Default: disabled")
                .define(FLUENTD_CLIENT_WAIT_UNTIL_BUFFER_FLUSHED, Type.INT, null, Importance.MEDIUM,
                        "Max wait until all buffers are flushed in sec. Default: 60sec")
                .define(FLUENTD_CLIENT_WAIT_UNTIL_FLUSHER_TERMINATED, Type.INT, null, Importance.MEDIUM,
                        "Max wait until the flusher is terminated in sec. Default: 60sec")
                .define(FLUENTD_CLIENT_JVM_HEAP_BUFFER_MODE, Type.BOOLEAN, false, Importance.MEDIUM,
                        "If true use JVM heap memory for buffer pool. Default: false");
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

    public Long getFluentdClientMaxBufferSize() {
        return getLong(FLUENTD_CLIENT_MAX_BUFFER_SIZE);
    }

    public Integer getFluentdClientBufferChunkInitialSize() {
        return getInt(FLUENTD_CLIENT_BUFFER_CHUNK_INITIAL_SIZE);
    }

    public Integer getFluentdClientBufferChunkRetentionSize() {
        return getInt(FLUENTD_CLIENT_BUFFER_CHUNK_RETENTION_SIZE);
    }

    public Integer getFluentdClientFlushInterval() {
        return getInt(FLUENTD_CLIENT_FLUSH_INTERVAL);
    }

    public boolean getFluentdClientAckResponseMode() {
        return getBoolean(FLUENTD_CLIENT_ACK_RESPONSE_MODE);
    }

    public String getFluentdClientFileBackupDir() {
        return getString(FLUENTD_CLIENT_FILE_BACKUP_DIR);
    }

    public Integer getFluentdClientWaitUntilBufferFlushed() {
        return getInt(FLUENTD_CLIENT_WAIT_UNTIL_BUFFER_FLUSHED);
    }

    public Integer getFluentdClientWaitUntilFlusherTerminated() {
        return getInt(FLUENTD_CLIENT_WAIT_UNTIL_FLUSHER_TERMINATED);
    }

    public boolean getFluentdClientJvmHeapBufferMode() {
        return getBoolean(FLUENTD_CLIENT_JVM_HEAP_BUFFER_MODE);
    }
}
