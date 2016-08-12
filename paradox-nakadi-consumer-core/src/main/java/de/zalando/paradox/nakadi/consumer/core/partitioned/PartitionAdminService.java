package de.zalando.paradox.nakadi.consumer.core.partitioned;

import java.util.List;
import java.util.Map;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;

public interface PartitionAdminService {
    List<EventType> getEventTypes();

    List<String> getConsumerNames();

    List<String> getEventConsumerNames(final EventType eventType);

    List<Map<String, Object>> getConsumerInfo();

    List<Map<String, Object>> getEventConsumerInfo(final EventType eventType);

    List<EventTypePartition> getEventPartitions(final EventType eventType);

    String getCustomerOffset(final String consumerName, final EventTypePartition eventTypePartition);

    void setCustomerOffset(final String consumerName, final EventTypeCursor cursor);

    void delCustomerOffset(final String consumerName, final EventTypePartition eventTypePartition);
}
