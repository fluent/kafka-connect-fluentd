package org.fluentd.kafka;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import influent.EventEntry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.List;
import java.util.Map;

public class FluentdSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(FluentdSourceTask.class);
    private ForwardServer server;
    private final ConcurrentLinkedDeque<SourceRecord> queue = new ConcurrentLinkedDeque<>();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        ForwardCallback callback = ForwardCallback.of(stream -> {
            stream.getEntries().forEach(entry -> {
                // TODO Construct SourceRecord
                SourceRecord record = new SourceRecord();
                this.queue.add(record);
            });
            return CompletableFuture.completedFuture(null);
        });
        // TODO configure server
        server = new ForwardServer
            .Builder(callback)
            .build();
        server.start();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<SourceRecord>();
        while (!queue.isEmpty()) {
            SourceRecord record = this.queue.poll();
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
