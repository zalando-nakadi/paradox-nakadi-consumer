package de.zalando.paradox.nakadi.consumer.core.exceptions;

public class UnrecoverableException extends RuntimeException {

    public UnrecoverableException() {
        super();
    }

    public UnrecoverableException(final String message) {
        super(message);
    }

    public UnrecoverableException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public UnrecoverableException(final Throwable cause) {
        super(cause);
    }
}
