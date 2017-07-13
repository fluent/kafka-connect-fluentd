package org.fluentd.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkRecordConverter {
    private static Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

    private RecordConverter schemafulRecordConverter = new SchemafulRecordConverter();
    private RecordConverter schemalessRecordConverter = new SchemalessRecordConverter();
    private RecordConverter rawJsonStringRecordConverter = new RawJsonStringRecordConverter();

    public FluentdEventRecord convert(SinkRecord sinkRecord) {
        logger.debug("SinkRecord: {}", sinkRecord);
        FluentdEventRecord eventRecord = null;

        if (sinkRecord.value() != null) {
            eventRecord = getRecordConverter(sinkRecord.valueSchema(), sinkRecord.value())
                    .convert(sinkRecord.valueSchema(), sinkRecord.value());
        }
        eventRecord.setTag(sinkRecord.topic());
        return eventRecord;
    }

    private RecordConverter getRecordConverter(Schema schema, Object data) {
        if (schema != null && data instanceof Struct) {
            logger.debug("Schemaful converter");
            return schemafulRecordConverter;
        }

        if (data instanceof Map) {
            logger.debug("Schemaless converter");
            return schemalessRecordConverter;
        }

        if (data instanceof String) {
            logger.debug("Raw converter");
            return rawJsonStringRecordConverter;
        }

        throw new DataException("No converter found due to unexpected object type " + data.getClass().getName());
    }
}
