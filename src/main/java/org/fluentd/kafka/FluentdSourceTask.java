package org.fluentd.kafka;

import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;

public class FluentdSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(FluentdSourceTask.class);
    private FluentdSourceConnectorConfig config;
    private ForwardServer server;
    private final ConcurrentLinkedDeque<SourceRecord> queue = new ConcurrentLinkedDeque<>();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new FluentdSourceConnectorConfig(properties);
        ForwardCallback callback = ForwardCallback.of(stream -> {
            stream.getEntries().forEach(entry -> {
                // Long timestamp = entry.getTime().toEpochMilli();
                EventEntryConverter c = new EventEntryConverter();
                Struct s = c.toStruct(entry);
                SourceRecord sourceRecord = new SourceRecord(
                        null,
                        null,
                        stream.getTag().getName(),
                        null,
                        Schema.STRING_SCHEMA,
                        stream.getTag().getName(),
                        s.schema(),
                        s
                );
                queue.add(sourceRecord);
            });
            return CompletableFuture.completedFuture(null);
        });
        // TODO configure server
        try {
            server = new ForwardServer
                    .Builder(callback)
                    .localAddress(config.getLocalAddress())
                    .build();
        } catch (FluentdConnectorConfigError ex) {
            throw new ConnectException(ex);
        }
        server.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        while (!queue.isEmpty()) {
            SourceRecord record = this.queue.poll();
            log.debug("{}", record);
            if (record != null) {
                records.add(record);
            }
        }
        return records;
    }

    @Override
    public void stop() {
        server.shutdown();
    }
}
