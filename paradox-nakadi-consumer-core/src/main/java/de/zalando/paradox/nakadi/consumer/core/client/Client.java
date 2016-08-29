package de.zalando.paradox.nakadi.consumer.core.client;

import java.util.List;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;

import rx.Single;

public interface Client {

    Single<List<NakadiPartition>> getPartitions(final EventType eventType);

    Single<String> getEvent(final EventTypeCursor cursor);

    Single<String> getContent(final EventTypeCursor cursor);
}
