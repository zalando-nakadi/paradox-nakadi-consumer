package de.zalando.paradox.nakadi.consumer.core.domain;

import java.util.Optional;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NakadiPartition {

    private final String partition;

    private final String oldestAvailableOffset;

    private final String newestAvailableOffset;

    private final Long unconsumedEvents;

    @JsonCreator
    public NakadiPartition(@JsonProperty("partition") final String partition,
            @JsonProperty("oldest_available_offset") final String oldestAvailableOffset,
            @JsonProperty("newest_available_offset") final String newestAvailableOffset,
            @Nullable
            @JsonProperty("unconsumed_events")
            final Long unconsumedEvents) {
        this.partition = partition;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
        this.unconsumedEvents = unconsumedEvents;
    }

    public String getPartition() {
        return partition;
    }

    public String getOldestAvailableOffset() {
        return oldestAvailableOffset;
    }

    public String getNewestAvailableOffset() {
        return newestAvailableOffset;
    }

    public Optional<Long> getUnconsumedEvents() {
        return Optional.ofNullable(unconsumedEvents);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("partition", partition)
                          .add("oldestAvailableOffset", oldestAvailableOffset)
                          .add("newestAvailableOffset", newestAvailableOffset).add("unconsumedEvents", unconsumedEvents)
                          .toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NakadiPartition that = (NakadiPartition) o;
        return Objects.equal(partition, that.partition)
                && Objects.equal(oldestAvailableOffset, that.oldestAvailableOffset)
                && Objects.equal(newestAvailableOffset, that.newestAvailableOffset)
                && Objects.equal(unconsumedEvents, that.unconsumedEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partition, oldestAvailableOffset, newestAvailableOffset, unconsumedEvents);
    }
}
