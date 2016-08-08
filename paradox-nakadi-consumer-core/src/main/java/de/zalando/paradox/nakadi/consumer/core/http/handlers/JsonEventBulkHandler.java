package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;

public interface JsonEventBulkHandler extends EventHandler<List<JsonNode>> { }
