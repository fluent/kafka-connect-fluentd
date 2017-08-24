package org.fluentd.kafka;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FluentdSourceConnectorTest {
    private FluentdSourceConnector connector;
    private ConnectorContext context;

    @Before
    public void setup() {
        connector = new FluentdSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);
    }

    @Test
    public void testSingleTask() {
        PowerMock.replayAll();
        connector.start(buildSourceProperties());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("24225", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_PORT));
        Assert.assertEquals("127.0.0.1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BIND));
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskConfigs() {
        PowerMock.replayAll();
        connector.start(buildSourceProperties());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("24225", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_PORT));
        Assert.assertEquals("127.0.0.1", taskConfigs.get(0).get(FluentdSourceConnectorConfig.FLUENTD_BIND));
        PowerMock.verifyAll();
    }

    private Map<String, String> buildSourceProperties() {
        final Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_PORT, "24225");
        sourceProperties.put(FluentdSourceConnectorConfig.FLUENTD_BIND, "127.0.0.1");
        return sourceProperties;
    }
}
