package de.zalando.paradox.nakadi.consumer.core.utils;

import java.net.SocketTimeoutException;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import com.google.common.base.Throwables;

import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;

public class ThrowableUtils {

    private static final Predicate<Throwable> UNRECOVERABLE_EXCEPTION_PREDICATE = t ->
            t instanceof UnrecoverableException;

    private static final Predicate<Throwable> ERROR_PREDICATE = t -> t instanceof Error;

    private static final Predicate<Throwable> UNRECOVERABLE_PREDICATE = UNRECOVERABLE_EXCEPTION_PREDICATE.or(
            ERROR_PREDICATE);

    private static final Predicate<Throwable> SOCKET_TIMEOUT_EXCEPTION_PREDICATE = t ->
            t instanceof SocketTimeoutException;

    private static final Predicate<Throwable> CONCURRENT_TIMEOUT_EXCEPTION_PREDICATE = t ->
            t instanceof TimeoutException;

    private static final Predicate<Throwable> HTTP_CONNECT_TIMEOUT_EXCEPTION_PREDICATE = t ->
            t instanceof org.apache.http.conn.ConnectTimeoutException;

    private static final Predicate<Throwable> TIMEOUT_PREDICATE = SOCKET_TIMEOUT_EXCEPTION_PREDICATE.or(
            CONCURRENT_TIMEOUT_EXCEPTION_PREDICATE).or(HTTP_CONNECT_TIMEOUT_EXCEPTION_PREDICATE);

    private ThrowableUtils() { }

    public static void throwException(final Throwable t) {
        ThrowableUtils.<RuntimeException>throwException0(t);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwException0(final Throwable t) throws E {
        throw (E) t;
    }

    public static boolean isUnrecoverableException(final Throwable exception) {
        return isException(exception, UNRECOVERABLE_PREDICATE);
    }

    public static boolean isTimeoutException(final Throwable exception) {
        return isException(exception, TIMEOUT_PREDICATE);
    }

    private static boolean isException(final Throwable exception, final Predicate<Throwable> predicate) {
        return Throwables.getCausalChain(exception).stream().filter(predicate).findFirst().isPresent();
    }
}
