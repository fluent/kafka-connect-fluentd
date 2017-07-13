package org.fluentd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RawJsonStringRecordConverter implements RecordConverter {
    @Override
    @SuppressWarnings("unchecked")
    public FluentdEventRecord convert(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        Map<String, Object> record;

        try {
            record = new ObjectMapper().readValue((String) value, LinkedHashMap.class);
        } catch (IOException e) {
            throw new DataException(e);
        }
        return new FluentdEventRecord(null, record);
    }
}
