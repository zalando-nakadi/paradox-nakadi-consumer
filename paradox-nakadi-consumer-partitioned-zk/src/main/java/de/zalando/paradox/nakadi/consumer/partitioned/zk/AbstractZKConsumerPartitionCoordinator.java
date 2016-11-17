package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionAdminService;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionOffsetManagement;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.AbstractPartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

abstract class AbstractZKConsumerPartitionCoordinator extends AbstractPartitionCoordinator {

    private boolean startNewestAvailableOffset = true;

    private final String consumerName;
    private final PartitionOffsetManagement offsetManagement;
    private final ZKConsumerOffset consumerOffset;
    private final ZKAdminService adminService;

    AbstractZKConsumerPartitionCoordinator(final Logger log, final ZKHolder zkHolder, final String consumerName,
            final List<EventErrorHandler> eventErrorHandlers) {
        super(log);
        this.consumerName = consumerName;
        this.consumerOffset = new ZKConsumerOffset(zkHolder, consumerName);
        this.offsetManagement = new ZKConsumerSyncOffsetManagement(this.consumerOffset, this, this, eventErrorHandlers);
        this.adminService = new ZKAdminService(zkHolder);
    }

    @Override
    public void commit(final EventTypeCursor cursor) {
        offsetManagement.commit(cursor);
    }

    @Override
    public void flush(final EventTypePartition eventTypePartition) {
        offsetManagement.flush(eventTypePartition);
    }

    @Override
    public void error(final Throwable t, final EventTypePartition eventTypePartition, @Nullable final String offset,
            final String rawEvent) {
        offsetManagement.error(t, eventTypePartition, offset, rawEvent);
    }

    @Override
    public void error(final int statusCode, final String content, final EventTypePartition eventTypePartition) {
        offsetManagement.error(statusCode, content, eventTypePartition);
    }

    Function<NakadiPartition, EventTypeCursor> getOffsetSelector(final EventType eventType) {
        return
            entry ->
                EventTypeCursor.of(EventTypePartition.of(eventType, entry.getPartition()),
                    getNextOffset(eventType, entry));
    }

    private String getNextOffset(final EventType eventType, final NakadiPartition nakadiPartition) {
        try {
            final String result = nextOffset(eventType, nakadiPartition);
            log.info("Next offset [{}] for event type [{}] , partition [{}]]", result, eventType,
                nakadiPartition.getPartition());
            return result;
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    private String nextOffset(final EventType eventType, final NakadiPartition nakadiPartition) throws Exception {
        String result = startNewestAvailableOffset ? nakadiPartition.getNewestAvailableOffset()
                                                   : nakadiPartition.getOldestAvailableOffset();

        final String path = consumerOffset.getOffsetPath(eventType.getName(), nakadiPartition.getPartition());
        final String zkOffset = consumerOffset.getOffset(path);
        if (null != zkOffset) {
            final long offsetValue;
            try {
                offsetValue = Long.parseLong(zkOffset);
            } catch (NumberFormatException e) {
                log.warn("Cannot convert ZK offset [{}] from path [{}] to long", zkOffset, path);
                return result;
            }

            final long newestValue;
            try {
                newestValue = Long.parseLong(nakadiPartition.getNewestAvailableOffset());
            } catch (NumberFormatException e) {
                log.warn("Cannot convert Nakadi newest offset [{}] for event type [{}] , partition [{}] to long",
                    nakadiPartition.getNewestAvailableOffset(), eventType, nakadiPartition.getPartition());
                return result;
            }

            long oldestValue = Long.MIN_VALUE;
            try {
                oldestValue = Long.parseLong(nakadiPartition.getOldestAvailableOffset());
            } catch (NumberFormatException ignore) {

                // ok when the value is BEGIN
                log.info("Cannot convert Nakadi oldest offset [{}] for event type [{}] , partition [{}] to long",
                    nakadiPartition.getOldestAvailableOffset(), eventType, nakadiPartition.getPartition());
            }

            // [[{"oldest_available_offset":"1522","newest_available_offset":"1521","partition":"0"}]]
            if (oldestValue > newestValue) {

                // messages purged due to retention time
                log.warn("Oldest offset [{}] greater than newest offset [{}]", oldestValue, newestValue);
                oldestValue = newestValue;
            }

            final long offset;
            if (oldestValue > offsetValue) {
                log.error("Inconsistent ZK data for [{}]. ZK offset [{}] is less than Nakadi oldest offset [{}]", path,
                    offsetValue, oldestValue);
                offset = oldestValue;
            } else if (offsetValue > newestValue) {
                log.error("Inconsistent ZK data for [{}]. ZK offset [{}] is greater than Nakadi newest offset [{}]",
                    path, offsetValue, oldestValue);
                offset = newestValue;
            } else {
                offset = offsetValue;
            }

            result = Long.toString(offset);

        }

        return result;
    }

    public void setStartNewestAvailableOffset(final boolean startNewestAvailableOffset) {
        this.startNewestAvailableOffset = startNewestAvailableOffset;
    }

    public String getConsumerName() {
        return consumerName;
    }

    @Override
    public Optional<PartitionAdminService> getAdminService() {
        return Optional.of(adminService);
    }
}
