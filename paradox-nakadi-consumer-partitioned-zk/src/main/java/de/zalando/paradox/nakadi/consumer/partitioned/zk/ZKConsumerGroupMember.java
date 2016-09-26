package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.lang.String.format;

import static java.util.Objects.requireNonNull;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import de.zalando.paradox.nakadi.consumer.core.domain.EventType;

class ZKConsumerGroupMember {
    private static final String CONSUMER_GROUP = "/paradox/nakadi/event_types/%s/consumers/%s/members";

    private final ZKMember member;

    private final ZKHolder zkHolder;

    private final String consumerName;

    ZKConsumerGroupMember(final ZKHolder zkHolder, final String consumerName, final ZKMember member) {
        this.zkHolder = requireNonNull(zkHolder, "zkHolder must not be null");
        this.consumerName = requireNonNull(consumerName, "consumerName must not be null");
        this.member = requireNonNull(member, "member must not be null");
    }

    interface GroupChangedListener {
        void memberAdded(final EventType eventType, final String memberId);

        void memberRemoved(final EventType eventType, final String memberId);
    }

    private PathChildrenCacheListener newListener(final EventType eventType, final GroupChangedListener delegate) {
        return
            (client, event) -> {
            switch (event.getType()) {

                case CHILD_ADDED : {
                    String addedMemberId = ZKPaths.getNodeFromPath(event.getData().getPath());
                    delegate.memberAdded(eventType, addedMemberId);
                    break;
                }

                case CHILD_REMOVED : {
                    final String removedMemberId = ZKPaths.getNodeFromPath(event.getData().getPath());
                    delegate.memberRemoved(eventType, removedMemberId);
                    break;
                }

                default :
                    break;
            }
        };
    }

    ZKGroupMember newGroupMember(final EventType eventType, final GroupChangedListener listener) {
        return new ZKGroupMember(zkHolder.getCurator(), getConsumerGroupPath(eventType.getName()), member.getMemberId(),
                member.toByteJson(), newListener(eventType, listener));
    }

    String getConsumerGroupPath(final String eventTypeName) {
        return format(CONSUMER_GROUP, eventTypeName, consumerName);
    }

    ZKMember getMember() {
        return member;
    }
}
