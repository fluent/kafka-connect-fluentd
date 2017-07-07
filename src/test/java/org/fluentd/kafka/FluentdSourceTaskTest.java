package org.fluentd.kafka;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.komamitsu.fluency.Fluency;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class FluentdSourceTaskTest {
    private FluentdSourceTask task;
    private Fluency fluency;

    @Before
    public void setUp() throws IOException {
        task = new FluentdSourceTask();
        fluency = Fluency.defaultFluency();
    }

    @After
    public void tearDown() {
        task.stop();
    }

    @Test
    public void oneRecord() throws InterruptedException, IOException {
        Map<String, String> config = new HashMap<>();
        task.start(config);
        Map<String, Object> record = new HashMap<>();
        record.put("message", "This is a test message");
        fluency.emit("test", record);
        Thread.sleep(1000);
        List<SourceRecord> sourceRecords = task.poll();
        assertEquals(1, sourceRecords.size());
    }
}