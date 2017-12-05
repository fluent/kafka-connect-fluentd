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

import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.apache.kafka.connect.data.Schema;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FluentdSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(FluentdSourceTask.class);
    private FluentdSourceConnectorConfig config;
    private ForwardServer server;
    private final ConcurrentLinkedDeque<SourceRecord> queue = new ConcurrentLinkedDeque<>();

    private static final class Reporter implements Runnable {
        private final AtomicLong counter = new AtomicLong();

        void add(final int up) {
            counter.addAndGet(up);
        }

        @Override
        public void run() {
            long lastChecked = System.currentTimeMillis();
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    break;
                }
                final long now = System.currentTimeMillis();
                if (now - lastChecked >= 1000) {
                    lastChecked = now;
                    final long current = counter.getAndSet(0);
                    log.info("{} requests/sec", current);
                }
            }
        }
    }

    private final static Reporter reporter = new Reporter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        config = new FluentdSourceConnectorConfig(properties);
        ForwardCallback callback = ForwardCallback.of(stream -> {
            if (config.getFluentdCounterEnabled()) {
                reporter.add(stream.getEntries().size());
            }
            stream.getEntries().forEach(entry -> {
                String topic = config.getFluentdStaticTopic();
                if (topic == null) {
                    topic = stream.getTag().getName();
                }
                Long timestamp = entry.getTime().toEpochMilli();
                SourceRecord sourceRecord = new SourceRecord(
                        null,
                        null,
                        topic,
                        null,
                        Schema.STRING_SCHEMA,
                        stream.getTag().getName(),
                        null,
                        entry.getRecord().toJson(),
                        timestamp
                );
                queue.add(sourceRecord);
            });
            // TODO complete this future when SourceTask#commit finishes
            return CompletableFuture.completedFuture(null);
        });
        try {
            if (!config.getFluentdTransport().equals("tcp") &&
                !config.getFluentdTransport().equals("tls")) {
                String message = FluentdSourceConnectorConfig.FLUENTD_TRANSPORT +
                    " must be \"tcp\" or \"tls\"";
                throw new FluentdConnectorConfigError(message);
            }
            ForwardServer.Builder builder = new ForwardServer
                    .Builder(callback)
                    .localAddress(config.getLocalAddress())
                    .chunkSizeLimit(config.getFluentdChunkSizeLimit())
                    .backlog(config.getFluentdBacklog())
                    .keepAliveEnabled(config.getFluentdKeepAliveEnabled())
                    .tcpNoDelayEnabled(config.getFluentdTcpNoDeleyEnabled())
                    .sslEnabled(config.getFluentdTransport().equals("tls"))
                    .tlsVersions(config.getFluentdTlsVersions().toArray(new String[0]))
                    .keystorePath(config.getFluentdKeystorePath())
                    .keystorePassword(config.getFluentdKeystorePassword())
                    .keyPassword(config.getFluentdKeyPassword());
            if (config.getFluentdSendBufferSize() != 0) {
                builder.sendBufferSize(config.getFluentdSendBufferSize());
            }
            if (config.getFluentdReceveBufferSize() != 0) {
                builder.receiveBufferSize(config.getFluentdReceveBufferSize());
            }
            if (config.getFluentdWorkerPoolSize() != 0) {
                builder.workerPoolSize(config.getFluentdWorkerPoolSize());
            }

            server = builder.build();
        } catch (FluentdConnectorConfigError ex) {
            throw new ConnectException(ex);
        }
        server.start();
        if (config.getFluentdCounterEnabled()) {
            new Thread(reporter).start();
        }
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
        if (records.isEmpty()) {
            synchronized (this) {
                this.wait(1000);
            }
        }
        return records;
    }

    @Override
    public void stop() {
        server.shutdown();
    }
}
