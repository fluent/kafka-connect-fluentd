package org.fluentd.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class FluentdSinkTask extends SinkTask {
    private static Logger log = LoggerFactory.getLogger(FluentdSinkTask.class);
    private Fluency fluency;
    private SinkRecordConverter converter;

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        //TODO: Create resources like database or api connections here.
        FluentdSinkConnectorConfig config = new FluentdSinkConnectorConfig(properties);
        Fluency.Config fluencyConfig = new Fluency.Config()
                .setMaxBufferSize(config.getFluentdClientMaxBufferSize())
                .setBufferChunkInitialSize(config.getFluentdClientBufferChunkInitialSize())
                .setBufferChunkRetentionSize(config.getFluentdClientBufferChunkRetentionSize())
                .setFlushIntervalMillis(config.getFluentdClientFlushInterval())
                .setAckResponseMode(config.getFluentdClientAckResponseMode())
                .setFileBackupDir(config.getFluentdClientFileBackupDir())
                .setWaitUntilBufferFlushed(config.getFluentdClientWaitUntilBufferFlushed())
                .setWaitUntilFlusherTerminated(config.getFluentdClientWaitUntilFlusherTerminated())
                .setJvmHeapBufferMode(config.getFluentdClientJvmHeapBufferMode());
        try {
            fluency = Fluency.defaultFluency(config.getFluentdConnectAddresses(), fluencyConfig);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
        converter = new SinkRecordConverter();
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        collection.forEach(sinkRecord -> {
            log.debug("key: {}, value: {}, class: {}, schema: {}",
                    sinkRecord.key(),
                    sinkRecord.value(),
                    sinkRecord.value().getClass().getCanonicalName(),
                    sinkRecord.valueSchema());
            // TODO fluency.emit(sinkRecord.key(), record);
            FluentdEventRecord eventRecord = converter.convert(sinkRecord);
            log.info("{}", eventRecord);
            try {
                if (eventRecord.getEventTime() != null) {
                    fluency.emit(eventRecord.getTag(), eventRecord.getEventTime(), eventRecord.getData());
                } else if (eventRecord.getTimestamp() != null) {
                    fluency.emit(eventRecord.getTag(), eventRecord.getTimestamp(), eventRecord.getData());
                } else {
                    fluency.emit(eventRecord.getTag(), eventRecord.getData());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        try {
            fluency.flush();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void stop() {
        try {
            fluency.waitUntilAllBufferFlushed(3);
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        }
    }

}
