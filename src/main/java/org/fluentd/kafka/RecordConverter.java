package org.fluentd.kafka;

import org.apache.kafka.connect.data.Schema;

public interface RecordConverter {
    FluentdEventRecord convert(Schema schema, Object value);
}
