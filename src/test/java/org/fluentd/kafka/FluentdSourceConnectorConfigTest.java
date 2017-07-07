package org.fluentd.kafka;

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class FluentdSourceConnectorConfigTest {
    @Test
    public void doc() {
        System.out.println(FluentdSourceConnectorConfig.conf().toRst());
    }

    @Test
    public void defaultValues() {
        Map<String, String> parsedConfig = new HashMap<>();
        FluentdSourceConnectorConfig conf = new FluentdSourceConnectorConfig(parsedConfig);
        assertEquals(24224, conf.getFluentdPort());
        assertEquals("0.0.0.0", conf.getFluentdBind());
    }

    @Test
    public void configValues() {
        Map<String, String> parsedConfig = new HashMap<>();
        parsedConfig.put(FluentdSourceConnectorConfig.FLUENTD_PORT, "24225");
        parsedConfig.put(FluentdSourceConnectorConfig.FLUENTD_BIND, "127.0.0.1");
        FluentdSourceConnectorConfig conf = new FluentdSourceConnectorConfig(parsedConfig);
        assertEquals(24225, conf.getFluentdPort());
        assertEquals("127.0.0.1", conf.getFluentdBind());
    }
}