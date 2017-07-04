package org.fluentd.kafka;

import org.junit.Test;

public class FluentdSinkConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(FluentdSinkConnectorConfig.conf().toRst());
  }
}
