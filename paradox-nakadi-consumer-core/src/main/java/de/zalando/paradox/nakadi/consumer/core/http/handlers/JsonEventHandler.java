package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import com.fasterxml.jackson.databind.JsonNode;

import de.zalando.paradox.nakadi.consumer.core.EventHandler;

public interface JsonEventHandler extends EventHandler<JsonNode> { }
