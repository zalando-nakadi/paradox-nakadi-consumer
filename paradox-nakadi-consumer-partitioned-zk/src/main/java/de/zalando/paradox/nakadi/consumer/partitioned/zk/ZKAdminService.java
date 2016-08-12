package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.lang.String.format;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.charset.StandardCharsets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypeCursor;
import de.zalando.paradox.nakadi.consumer.core.domain.EventTypePartition;
import de.zalando.paradox.nakadi.consumer.core.partitioned.PartitionAdminService;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

class ZKAdminService implements PartitionAdminService {
    private static final String CONSUMERS_PATH = "/paradox/nakadi/event_types/%s/partitions/%s/consumers/%s";
    private static final String PARTITIONS_PATH = "/paradox/nakadi/event_types/%s/partitions";
    private static final String EVENT_TYPES_PATH = "/paradox/nakadi/event_types";
    private static final Pattern SPLITTER = Pattern.compile("/?\\*/?");
    private static final String MEMBERS_PATH = "/paradox/nakadi/event_types/%s/consumers/%s/members/%s";

    private final ZKHolder zkHolder;
    private final CuratorFramework client;

    ZKAdminService(final ZKHolder zkHolder) {
        this.client = zkHolder.getCurator();
        this.zkHolder = zkHolder;
    }

    @Override
    public List<EventType> getEventTypes() {
        final List<String> children = getChildrenPath(EVENT_TYPES_PATH);
        return children.stream().distinct().sorted().map(EventType::of).collect(Collectors.toList());
    }

    @Override
    public List<String> getConsumerNames() {
        final List<String> children = getChildrenPath(getConsumersPath("*", "*", "*"));
        return children.stream().distinct().sorted().collect(Collectors.toList());
    }

    @Override
    public List<String> getEventConsumerNames(final EventType eventType) {
        checkArgument(null != eventType, "eventType must not be null");

        final List<String> children = getChildrenPath(getConsumersPath(eventType.getName(), "*", "*"));
        return children.stream().distinct().sorted().collect(Collectors.toList());
    }

    @Override
    public List<EventTypePartition> getEventPartitions(final EventType eventType) {
        checkArgument(null != eventType, "eventType must not be null");

        final List<String> children = getChildrenPath(format(PARTITIONS_PATH, eventType.getName()));
        return children.stream().distinct().sorted().map(partition -> EventTypePartition.of(eventType, partition))
                       .collect(Collectors.toList());
    }

    @Override
    public List<Map<String, Object>> getConsumerInfo() {
        final List<String> paths = getFullPath(getMemberPath("*", "*", "*"));
        return getConsumerInfoList(paths);
    }

    @Override
    public List<Map<String, Object>> getEventConsumerInfo(final EventType eventType) {
        checkArgument(null != eventType, "eventType must not be null");

        final List<String> paths = getFullPath(getMemberPath(eventType.getName(), "*", "*"));
        return getConsumerInfoList(paths);
    }

    private List<Map<String, Object>> getConsumerInfoList(final List<String> paths) {
        return paths.stream().map(this::getMemberInfo).filter(Objects::nonNull).collect(Collectors.toList());
    }

    private Map<String, Object> getMemberInfo(final String path) {
        try {
            final byte[] data = client.getData().forPath(path);
            if (null != data && data.length != 0) {
                final HashMap<String, Object> map =
                    new ObjectMapper().readValue(new String(data, StandardCharsets.UTF_8),
                        new TypeReference<HashMap<String, Object>>() { });

                final String eventName = getPathPart(path, 4);
                if (StringUtils.isNotEmpty(eventName)) {
                    map.put("eventName", eventName);
                }

                final String consumerName = getPathPart(path, 6);
                if (StringUtils.isNotEmpty(consumerName)) {
                    map.put("consumerName", consumerName);
                }

                return map;
            } else {
                return null;
            }
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    private String getPathPart(final String path, final int index) {
        final String[] parts = path.split("/");
        return index < parts.length ? parts[index] : null;
    }

    @Override
    public String getCustomerOffset(final String consumerName, final EventTypePartition eventTypePartition) {
        try {
            return new ZKConsumerOffset(zkHolder, consumerName).getOffset(eventTypePartition);
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
            return null;
        }
    }

    @Override
    public void setCustomerOffset(final String consumerName, final EventTypeCursor cursor) {
        try {
            new ZKConsumerOffset(zkHolder, consumerName).setOffset(cursor);
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
        }
    }

    @Override
    public void delCustomerOffset(final String consumerName, final EventTypePartition eventTypePartition) {
        try {
            new ZKConsumerOffset(zkHolder, consumerName).delOffset(eventTypePartition);
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
        }
    }

    private static String getMemberPath(final String eventName, final String consumerName, final String memberId) {
        return format(MEMBERS_PATH, eventName, consumerName, memberId);
    }

    private static String getConsumersPath(final String eventName, final String partition, final String consumerName) {
        return format(CONSUMERS_PATH, eventName, partition, consumerName);
    }

    private List<String> getChildrenPath(final String pattern) {
        final String[] paths = SPLITTER.split(pattern);
        if (paths.length == 0) {
            return getChildren(paths[0]);
        } else if (paths.length > 0) {
            return getNextChildrenPath(paths, 0, paths[0]);
        } else {
            return Collections.emptyList();
        }
    }

    private List<String> getNextChildrenPath(final String[] paths, final int index, final String prefix) {
        final boolean last = index >= paths.length - 1;
        if (last) {
            return getChildren(prefix);
        } else {
            final int nextIndex = index + 1;
            final String nextPath = paths[nextIndex];
            return getChildren(prefix).stream().map(child -> {
                                          final String childPrefix = prefix + "/" + child + "/" + nextPath;
                                          return getNextChildrenPath(paths, nextIndex, childPrefix);
                                      }).flatMap(Collection::stream).collect(Collectors.toList());
        }
    }

    private List<String> getFullPath(final String pattern) {
        final String[] paths = SPLITTER.split(pattern);
        if (paths.length == 0) {
            return getLastFullPath(paths[0]);
        } else if (paths.length > 0) {
            return getNextFullPath(paths, 0, paths[0]);
        } else {
            return Collections.emptyList();
        }
    }

    private List<String> getNextFullPath(final String[] paths, final int index, final String prefix) {
        final boolean last = index >= paths.length - 1;
        if (last) {
            return getLastFullPath(prefix);
        } else {
            final int nextIndex = index + 1;
            final String nextPath = paths[nextIndex];
            return getChildren(prefix).stream().map(child -> {
                                          final String childPrefix = prefix + "/" + child + "/" + nextPath;
                                          return getNextFullPath(paths, nextIndex, childPrefix);
                                      }).flatMap(Collection::stream).collect(Collectors.toList());
        }
    }

    private List<String> getLastFullPath(final String path) {
        return getChildren(path).stream().map(child -> ZKPaths.makePath(path, child)).collect(Collectors.toList());
    }

    private List<String> getChildren(final String path) {
        try {
            return client.checkExists().forPath(path) != null ? client.getChildren().forPath(path)
                                                              : Collections.emptyList();
        } catch (Exception e) {
            ThrowableUtils.throwException(e);
            return Collections.emptyList();
        }
    }
}
