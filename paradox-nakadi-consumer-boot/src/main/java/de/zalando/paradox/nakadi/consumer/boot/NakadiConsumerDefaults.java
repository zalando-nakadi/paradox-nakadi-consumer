package de.zalando.paradox.nakadi.consumer.boot;

public class NakadiConsumerDefaults {

    private String zookeeperBrokers;

    private String exhibitorAddresses;

    private Integer exhibitorPort = 8181;

    private String defaultConsumerName = "default-consumer";

    private String nakadiUrl;

    private String nakadiTokenId = "nakadi-event-stream-read";

    private boolean oauth2Enabled;

    private String partitionCoordinatorProvider;

    private Long partitionsRetryAfterMillis;

    private Long partitionsTimeoutMillis;

    private Long partitionsRetryRandomMillis;

    private Long eventsTimeoutMillis;

    private Long eventsRetryAfterMillis;

    private Long eventsRetryRandomMillis;

    private Integer eventsStreamTimeoutSeconds;

    private Integer eventsStreamLimit;

    private Integer eventsStreamKeepAliveLimit;

    private Integer eventsBatchTimeoutSeconds;

    private Integer eventsBatchLimit;

    private boolean startNewestAvailableOffset = true;

    private boolean deleteUnavailableCursors;

    private boolean eventTypePartitionCoordinator = true;

    public String getZookeeperBrokers() {
        return zookeeperBrokers;
    }

    public void setZookeeperBrokers(final String zookeeperBrokers) {
        this.zookeeperBrokers = zookeeperBrokers;
    }

    public String getDefaultConsumerName() {
        return defaultConsumerName;
    }

    public void setDefaultConsumerName(final String defaultConsumerName) {
        this.defaultConsumerName = defaultConsumerName;
    }

    public String getNakadiUrl() {
        return nakadiUrl;
    }

    public void setNakadiUrl(final String nakadiUrl) {
        this.nakadiUrl = nakadiUrl;
    }

    public boolean isOauth2Enabled() {
        return oauth2Enabled;
    }

    public void setOauth2Enabled(final boolean oauth2Enabled) {
        this.oauth2Enabled = oauth2Enabled;
    }

    public String getPartitionCoordinatorProvider() {
        return partitionCoordinatorProvider;
    }

    public void setPartitionCoordinatorProvider(final String partitionCoordinatorProvider) {
        this.partitionCoordinatorProvider = partitionCoordinatorProvider;
    }

    public String getExhibitorAddresses() {
        return exhibitorAddresses;
    }

    public void setExhibitorAddresses(final String exhibitorAddresses) {
        this.exhibitorAddresses = exhibitorAddresses;
    }

    public Integer getExhibitorPort() {
        return exhibitorPort;
    }

    public void setExhibitorPort(final Integer exhibitorPort) {
        this.exhibitorPort = exhibitorPort;
    }

    public Long getPartitionsRetryAfterMillis() {
        return partitionsRetryAfterMillis;
    }

    public void setPartitionsRetryAfterMillis(final Long partitionsRetryAfterMillis) {
        this.partitionsRetryAfterMillis = partitionsRetryAfterMillis;
    }

    public Long getPartitionsTimeoutMillis() {
        return partitionsTimeoutMillis;
    }

    public void setPartitionsTimeoutMillis(final Long partitionsTimeoutMillis) {
        this.partitionsTimeoutMillis = partitionsTimeoutMillis;
    }

    public Long getEventsTimeoutMillis() {
        return eventsTimeoutMillis;
    }

    public void setEventsTimeoutMillis(final Long eventsTimeoutMillis) {
        this.eventsTimeoutMillis = eventsTimeoutMillis;
    }

    public Long getEventsRetryAfterMillis() {
        return eventsRetryAfterMillis;
    }

    public void setEventsRetryAfterMillis(final Long eventsRetryAfterMillis) {
        this.eventsRetryAfterMillis = eventsRetryAfterMillis;
    }

    public String getNakadiTokenId() {
        return nakadiTokenId;
    }

    public void setNakadiTokenId(final String nakadiTokenId) {
        this.nakadiTokenId = nakadiTokenId;
    }

    public Integer getEventsStreamTimeoutSeconds() {
        return eventsStreamTimeoutSeconds;
    }

    public void setEventsStreamTimeoutSeconds(final Integer eventsStreamTimeoutSeconds) {
        this.eventsStreamTimeoutSeconds = eventsStreamTimeoutSeconds;
    }

    public Integer getEventsStreamLimit() {
        return eventsStreamLimit;
    }

    public void setEventsStreamLimit(final Integer eventsStreamLimit) {
        this.eventsStreamLimit = eventsStreamLimit;
    }

    public Integer getEventsStreamKeepAliveLimit() {
        return eventsStreamKeepAliveLimit;
    }

    public void setEventsStreamKeepAliveLimit(final Integer eventsStreamKeepAliveLimit) {
        this.eventsStreamKeepAliveLimit = eventsStreamKeepAliveLimit;
    }

    public Integer getEventsBatchTimeoutSeconds() {
        return eventsBatchTimeoutSeconds;
    }

    public void setEventsBatchTimeoutSeconds(final Integer eventsBatchTimeoutSeconds) {
        this.eventsBatchTimeoutSeconds = eventsBatchTimeoutSeconds;
    }

    public Integer getEventsBatchLimit() {
        return eventsBatchLimit;
    }

    public void setEventsBatchLimit(final Integer eventsBatchLimit) {
        this.eventsBatchLimit = eventsBatchLimit;
    }

    public boolean isStartNewestAvailableOffset() {
        return startNewestAvailableOffset;
    }

    public void setStartNewestAvailableOffset(final boolean startNewestAvailableOffset) {
        this.startNewestAvailableOffset = startNewestAvailableOffset;
    }

    public boolean isDeleteUnavailableCursors() {
        return deleteUnavailableCursors;
    }

    public void setDeleteUnavailableCursors(final boolean deleteUnavailableCursors) {
        this.deleteUnavailableCursors = deleteUnavailableCursors;
    }

    public Long getPartitionsRetryRandomMillis() {
        return partitionsRetryRandomMillis;
    }

    public void setPartitionsRetryRandomMillis(final Long partitionsRetryRandomMillis) {
        this.partitionsRetryRandomMillis = partitionsRetryRandomMillis;
    }

    public Long getEventsRetryRandomMillis() {
        return eventsRetryRandomMillis;
    }

    public void setEventsRetryRandomMillis(final Long eventsRetryRandomMillis) {
        this.eventsRetryRandomMillis = eventsRetryRandomMillis;
    }

    public boolean isEventTypePartitionCoordinator() {
        return eventTypePartitionCoordinator;
    }

    public void setEventTypePartitionCoordinator(final boolean eventTypePartitionCoordinator) {
        this.eventTypePartitionCoordinator = eventTypePartitionCoordinator;
    }
}
