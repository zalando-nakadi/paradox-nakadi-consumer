package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.util.List;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;

public interface BatchEventsBulkHandler<T> extends EventHandler<List<T>>, EventClassProvider<T> { }
