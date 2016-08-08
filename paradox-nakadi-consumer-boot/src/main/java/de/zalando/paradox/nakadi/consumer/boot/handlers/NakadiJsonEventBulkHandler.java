package de.zalando.paradox.nakadi.consumer.boot.handlers;

import de.zalando.paradox.nakadi.consumer.boot.NakadiEventHandler;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.JsonEventBulkHandler;

public interface NakadiJsonEventBulkHandler extends JsonEventBulkHandler, NakadiEventHandler { }
