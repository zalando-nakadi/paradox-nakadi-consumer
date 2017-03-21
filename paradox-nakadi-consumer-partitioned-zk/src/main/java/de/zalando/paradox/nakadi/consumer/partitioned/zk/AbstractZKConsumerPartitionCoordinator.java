package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import com.google.common.base.MoreObjects;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.domain.NakadiPartition;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.EventErrorHandler;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionAdminService;
import de.zalando.paradox.nakadi.consumer.core.partitioned.impl.AbstractPartitionCoordinator;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

abstract class AbstractZKConsumerPartitionCoordinator extends AbstractPartitionCoordinator {

    private volatile boolean startNewestAvailableOffset = true;

    private final String consumerName;

    private final ZKConsumerSyncOffsetManagement offsetManagement;

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
        final String newestOrOldestAvailableOffset = startNewestAvailableOffset
            ? nakadiPartition.getNewestAvailableOffset() : "BEGIN";

        final String zkOffset = consumerOffset.getOffset(eventType, nakadiPartition.getPartition());

        return MoreObjects.firstNonNull(zkOffset, newestOrOldestAvailableOffset);
    }

    public void setStartNewestAvailableOffset(final boolean startNewestAvailableOffset) {
        this.startNewestAvailableOffset = startNewestAvailableOffset;
    }

    public void setDeleteUnavailableCursors(final boolean deleteUnavailableCursors) {
        this.offsetManagement.setDeleteUnavailableCursors(deleteUnavailableCursors);
    }

    public String getConsumerName() {
        return consumerName;
    }

    @Override
    public Optional<PartitionAdminService> getAdminService() {
        return Optional.of(adminService);
    }

}
