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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.komamitsu.fluency.EventTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkRecordConverter {
    private static Logger logger = LoggerFactory.getLogger(SinkRecordConverter.class);

    private final FluentdSinkConnectorConfig config;

    private RecordConverter schemafulRecordConverter = new SchemafulRecordConverter();
    private RecordConverter schemalessRecordConverter = new SchemalessRecordConverter();
    private RecordConverter rawJsonStringRecordConverter = new RawJsonStringRecordConverter();

    public SinkRecordConverter(final FluentdSinkConnectorConfig config) {
        this.config = config;
    }

    public FluentdEventRecord convert(SinkRecord sinkRecord) {
        logger.debug("SinkRecord: {}", sinkRecord);
        FluentdEventRecord eventRecord = null;

        if (sinkRecord.value() != null) {
            eventRecord = getRecordConverter(sinkRecord.valueSchema(), sinkRecord.value())
                    .convert(sinkRecord.valueSchema(), sinkRecord.value());
        }
        eventRecord.setTag(sinkRecord.topic());

        if (config.getFluentdClientTimestampInteger()) {
            eventRecord.setTimestamp(sinkRecord.timestamp() / 1000);
        } else {
            eventRecord.setEventTime(EventTime.fromEpochMilli(sinkRecord.timestamp()));
        }

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
