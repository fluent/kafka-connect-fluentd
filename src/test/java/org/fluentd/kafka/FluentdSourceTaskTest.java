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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FluentdSourceTaskTest {
    private FluentdSourceTask task;
    private Fluency fluency;

    @Before
    public void setUp() throws IOException {
        task = new FluentdSourceTask();
        fluency = Fluency.defaultFluency();
    }

    @After
    public void tearDown() throws InterruptedException {
        task.stop();
        Thread.sleep(500);
    }

    @Test
    public void oneRecord() throws InterruptedException, IOException {
        Map<String, String> config = new HashMap<>();
        config.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "false");
        task.start(config);
        Map<String, Object> record = new HashMap<>();
        record.put("message", "This is a test message");
        fluency.emit("test", record);
        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(1, sourceRecords.size());
        SourceRecord sourceRecord = sourceRecords.get(0);
        assertNull(sourceRecord.key());
        assertNull(sourceRecord.valueSchema());
        assertEquals(record, sourceRecord.value());
    }

    @Test
    public void oneRecordWithNullValue() throws InterruptedException, IOException {
        Map<String, String> config = new HashMap<>();
        config.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "false");
        task.start(config);
        Map<String, Object> record = new HashMap<>();
        record.put("message", null);
        fluency.emit("test", record);
        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(1, sourceRecords.size());
        SourceRecord sourceRecord = sourceRecords.get(0);
        assertNull(sourceRecord.key());
        assertNull(sourceRecord.valueSchema());
        assertEquals(record, sourceRecord.value());
    }

    @Test
    public void nestedRecord() throws IOException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "false");
        task.start(config);
        Map<String, Double> version = new HashMap<>();
        version.put("stable", 0.12);
        version.put("unstable", 0.14);
        List<String> versions = new ArrayList<>();
        versions.add("v0.12");
        versions.add("v0.14");
        Map<String, Object> record = new HashMap<>();
        record.put("versions", versions);
        record.put("version", version);
        // {"versions": ["v0.12", "v0.14"], version: {"stable": 0.12, "unstable": 0.14}}
        fluency.emit("test", record);
        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(1, sourceRecords.size());
        SourceRecord sourceRecord = sourceRecords.get(0);
        assertNull(sourceRecord.key());
        Map<String, Object> value = (Map<String, Object>) sourceRecord.value();
        assertThat((List<String>) value.get("versions"), hasItems("v0.12", "v0.14"));
        assertEquals(value.get("version"), version);
    }

    @Test
    public void multipleRecords() throws InterruptedException, IOException {
        Map<String, String> config = new HashMap<>();
        config.put(FluentdSourceConnectorConfig.FLUENTD_SCHEMAS_ENABLE, "false");
        task.start(config);
        Map<String, Object> record1 = new HashMap<>();
        record1.put("message", "This is a test message1");
        Map<String, Object> record2 = new HashMap<>();
        record2.put("message", "This is a test message2");
        fluency.emit("test", record1);
        fluency.emit("test", record2);
        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(2, sourceRecords.size());
        assertNull(sourceRecords.get(0).valueSchema());
        Map<String, Object> value1 = (Map<String, Object>) sourceRecords.get(0).value();
        assertEquals("This is a test message1", value1.get("message"));
        assertNull(sourceRecords.get(1).valueSchema());
        Map<String, Object> value2 = (Map<String, Object>) sourceRecords.get(1).value();
        assertEquals("This is a test message2", value2.get("message"));
    }
}
