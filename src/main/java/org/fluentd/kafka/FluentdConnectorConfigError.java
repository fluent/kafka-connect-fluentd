package org.fluentd.kafka;

public class FluentdConnectorConfigError extends Throwable {
    private String message;
    public FluentdConnectorConfigError(String message) {
        this.message = message;
    }
}
