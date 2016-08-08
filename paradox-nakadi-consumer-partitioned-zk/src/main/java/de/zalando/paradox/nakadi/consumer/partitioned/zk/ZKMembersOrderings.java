package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

public class ZKMembersOrderings {

    public static final ZKLeaderConsumerPartitionRebalanceStrategy.MembersOrdering MEMBERS_ID_ORDER =
        currentMembers -> {
        final String[] membersIdsArray = currentMembers.keySet().toArray(new String[currentMembers.size()]);
        Arrays.sort(membersIdsArray);
        return membersIdsArray;
    };

    private static long created(final Map.Entry<String, ZKMember> entry) {
        return null != entry.getValue() ? entry.getValue().getCreated() : 0L;
    }

    private static String[] getSortedMembers(final Map<String, ZKMember> currentMembers,
            final Comparator<? super Map.Entry<String, ZKMember>> comparator) {
        return currentMembers.entrySet().stream().sorted(comparator).map(Map.Entry::getKey).toArray(String[]::new);
    }

    public static final ZKLeaderConsumerPartitionRebalanceStrategy.MembersOrdering OLDEST_MEMBERS = currentMembers -> {
        final Comparator<? super Map.Entry<String, ZKMember>> comparator = new Ordering<Map.Entry<String, ZKMember>>() {
            @Override
            public int compare(@Nonnull final Map.Entry<String, ZKMember> left,
                    @Nonnull final Map.Entry<String, ZKMember> right) {
                return ComparisonChain.start().compare(created(left), created(right))
                                      .compare(left.getKey(), right.getKey()).result();
            }
        };
        return getSortedMembers(currentMembers, comparator);
    };

    public static final ZKLeaderConsumerPartitionRebalanceStrategy.MembersOrdering NEWEST_MEMBERS = currentMembers -> {

        final Comparator<? super Map.Entry<String, ZKMember>> comparator = new Ordering<Map.Entry<String, ZKMember>>() {
            @Override
            public int compare(@Nonnull final Map.Entry<String, ZKMember> left,
                    @Nonnull final Map.Entry<String, ZKMember> right) {
                return ComparisonChain.start().compare(created(right), created(left))
                                      .compare(left.getKey(), right.getKey()).result();
            }
        };
        return getSortedMembers(currentMembers, comparator);
    };
}
