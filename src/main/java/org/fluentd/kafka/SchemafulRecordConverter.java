package org.fluentd.kafka;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SchemafulRecordConverter implements RecordConverter {

    private final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
            Arrays.asList(
                    Date.LOGICAL_NAME,
                    Decimal.LOGICAL_NAME,
                    Time.LOGICAL_NAME,
                    Timestamp.LOGICAL_NAME)
    );

    @Override
    public FluentdEventRecord convert(Schema schema, Object value) {
        Map<Object, Object> map = toMap(schema, (Struct) value);
        Map<String, Object> record = new LinkedHashMap<>();
        map.forEach((key, val) -> record.put(key.toString(), val));

        return new FluentdEventRecord(null, record);
    }

    private Map<Object, Object> toMap(Schema schema, Object value) {
        Map<Object, Object> map = new LinkedHashMap<>();
        schema.fields().forEach(field -> processField(map, (Struct) value, field));
        return map;
    }

    private void processField(Map<Object, Object> map, Struct struct, Field field) {
        if (isSupportedLogicalType(field.schema())) {
            map.put(field.name(), struct.get(field));
        }

        switch (field.schema().type()) {
            case BOOLEAN:
            case FLOAT32:
            case FLOAT64:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case BYTES:
            case STRING: {
                map.put(field.name(), struct.get(field));
                break;
            }
            case ARRAY: {
                List<Object> list = new ArrayList<>();
                struct.getArray(field.name()).forEach(element -> {
                    list.add(processValue(field.schema().valueSchema(), element));
                });
                map.put(field.name(), list);
                break;
            }
            case MAP: {
                Map<Object, Object> innerMap = new LinkedHashMap<>();
                struct.getMap(field.name()).forEach((key, value) -> {
                    innerMap.put(processValue(field.schema().keySchema(), key),
                            processValue(field.schema().valueSchema(), value));
                });
                map.put(field.name(), innerMap);
                break;
            }
            case STRUCT: {
                Struct s = struct.getStruct(field.name());
                Map<Object, Object> innerMap = new LinkedHashMap<>();
                s.schema().fields().forEach(f -> processField(innerMap, s, f));
                break;
            }
            default:
                throw new DataException("Unknown schema type: " + field.schema().type());
        }
    }

    private Object processValue(Schema schema, Object value) {
        switch (schema.type()) {
            case BOOLEAN:
            case FLOAT32:
            case FLOAT64:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case BYTES:
            case STRING:
                return value;
            case MAP:
            case ARRAY:
            case STRUCT:
                return new DataException("Unsupported schema type: " + schema.type());
            default:
                throw new DataException("Unknown schema type: " + schema.type());
        }
    }

    private boolean isSupportedLogicalType(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.name() == null) {
            return false;
        }

        return LOGICAL_TYPE_NAMES.contains(schema.name());
    }
}
