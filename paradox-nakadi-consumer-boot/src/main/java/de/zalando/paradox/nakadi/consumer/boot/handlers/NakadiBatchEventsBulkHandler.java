package de.zalando.paradox.nakadi.consumer.boot.handlers;

import de.zalando.paradox.nakadi.consumer.boot.NakadiEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsBulkHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.BatchEventsHandler;

public interface NakadiBatchEventsBulkHandler<T> extends BatchEventsBulkHandler<T>, NakadiEventHandler { }
