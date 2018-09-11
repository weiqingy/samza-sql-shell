package org.apache.samza.tools.client.util;


public class CliException extends RuntimeException {
    public CliException() {

    }

    public CliException(String message) {
        super(message);
    }

    public CliException(String message, Throwable cause) {
        super(message, cause);
    }

    public CliException(Throwable cause) {
        super(cause);
    }
}
