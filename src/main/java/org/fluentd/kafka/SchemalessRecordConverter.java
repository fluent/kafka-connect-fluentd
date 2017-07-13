package org.fluentd.kafka;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashMap;
import java.util.Map;

public class SchemalessRecordConverter implements RecordConverter {
    @Override
    public FluentdEventRecord convert(Schema schema, Object value) {
        if (value instanceof Map) {
            return convertMap((Map<?, ?>)value);
        }
        // TODO support other types

        return null;
    }

    private FluentdEventRecord convertMap(Map<?, ?>map) {
        Map<String, Object> record = new LinkedHashMap<>();
        map.forEach((key, val) -> {
            record.put(key.toString(), val);
        });
        return new FluentdEventRecord(null, record);
    }
}
