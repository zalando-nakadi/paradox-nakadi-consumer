package de.zalando.paradox.nakadi.consumer.core.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NakadiPartition {

    private final String partition;

    private final String oldestAvailableOffset;

    private final String newestAvailableOffset;

    public NakadiPartition(@JsonProperty("partition") final String partition,
            @JsonProperty("oldest_available_offset") final String oldestAvailableOffset,
            @JsonProperty("newest_available_offset") final String newestAvailableOffset) {
        this.partition = partition;
        this.oldestAvailableOffset = oldestAvailableOffset;
        this.newestAvailableOffset = newestAvailableOffset;
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("partition", partition)
                          .add("oldestAvailableOffset", oldestAvailableOffset)
                          .add("newestAvailableOffset", newestAvailableOffset).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NakadiPartition that = (NakadiPartition) o;
        return Objects.equal(partition, that.partition) &&
                Objects.equal(oldestAvailableOffset, that.oldestAvailableOffset) &&
                Objects.equal(newestAvailableOffset, that.newestAvailableOffset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(partition, oldestAvailableOffset, newestAvailableOffset);
    }
}
