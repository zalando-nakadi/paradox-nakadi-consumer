package de.zalando.paradox.nakadi.consumer.core.exceptions;

public class InvalidEventTypeException extends UnrecoverableException {
    public InvalidEventTypeException() {
        super();
    }

    public InvalidEventTypeException(final String message) {
        super(message);
    }

    public InvalidEventTypeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public InvalidEventTypeException(final Throwable cause) {
        super(cause);
    }
}
