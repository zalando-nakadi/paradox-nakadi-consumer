package de.zalando.paradox.nakadi.consumer.core.utils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public class LoggingUtils {

    public static Logger getLogger(final Class<?> clazz, final EventTypePartition eventTypePartition) {
        return getLogger(clazz, null, eventTypePartition);
    }

    public static Logger getLogger(final Class<?> clazz, final String infix,
            final EventTypePartition eventTypePartition) {
        final String loggerName;
        if (StringUtils.isNotEmpty(infix)) {
            loggerName = clazz.getName() + "." + infix + "." + eventTypePartition.getEventType().getName() + "."
                    + eventTypePartition.getPartition();
        } else {
            loggerName = clazz.getName() + "." + eventTypePartition.getEventType().getName() + "."
                    + eventTypePartition.getPartition();
        }

        return LoggerFactory.getLogger(loggerName);
    }

    public static Logger getLogger(final Class<?> clazz, final EventType eventType) {
        return getLogger(clazz, null, eventType);
    }

    public static Logger getLogger(final Class<?> clazz, final String infix, final EventType eventType) {
        final String loggerName;
        if (StringUtils.isNotEmpty(infix)) {
            loggerName = clazz.getName() + "." + infix + "." + eventType.getName();
        } else {
            loggerName = clazz.getName() + "." + eventType.getName();
        }

        return LoggerFactory.getLogger(loggerName);
    }

    public static Logger getLogger(final Class<?> clazz, final String name) {
        final String loggerName = clazz.getName() + "." + name;
        return LoggerFactory.getLogger(loggerName);
    }
}
