package org.fluentd.kafka;

import org.junit.Test;

public class FluentdSourceConnectorConfigTest {
  @Test
  public void doc() {
    System.out.println(FluentdSourceConnectorConfig.conf().toRst());
  }
}