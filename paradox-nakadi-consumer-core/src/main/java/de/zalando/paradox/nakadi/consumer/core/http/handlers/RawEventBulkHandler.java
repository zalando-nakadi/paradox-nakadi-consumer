package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.util.List;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;

public interface RawEventBulkHandler extends EventHandler<List<String>> { }
