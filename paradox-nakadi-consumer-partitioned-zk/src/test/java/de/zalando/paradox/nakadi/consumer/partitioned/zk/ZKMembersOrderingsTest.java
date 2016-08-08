package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class ZKMembersOrderingsTest {
    @Test
    public void testOrderByMemberId() {
        Map<String, ZKMember> currentMembers = new Builder().add("2", 9100).add("3", 9009).add("1", 1234).add("4", 1234)
                                                            .build();

        final String[] membersIds = ZKMembersOrderings.MEMBERS_ID_ORDER.getOrderedMembersIds(currentMembers);
        Assert.assertArrayEquals(new String[] {"1", "2", "3", "4"}, membersIds);
    }

    @Test
    public void testOrderByOldestMember() {
        Map<String, ZKMember> currentMembers = new Builder().add("2", 20).add("5", 50).add("3", 30).add("1", 10)
                                                            .add("4", 30).build();

        final String[] membersIds = ZKMembersOrderings.OLDEST_MEMBERS.getOrderedMembersIds(currentMembers);
        Assert.assertArrayEquals(new String[] {"1", "2", "3", "4", "5"}, membersIds);
    }

    @Test
    public void testOrderByNewestMember() {
        Map<String, ZKMember> currentMembers = new Builder().add("4", 20).add("1", 50).add("3", 30).add("5", 10)
                                                            .add("2", 30).build();

        final String[] membersIds = ZKMembersOrderings.NEWEST_MEMBERS.getOrderedMembersIds(currentMembers);
        Assert.assertArrayEquals(new String[] {"1", "2", "3", "4", "5"}, membersIds);
    }

    static class Builder {
        private final Map<String, ZKMember> members = new HashMap<>();

        final Builder add(final String memberId, final long created) {
            members.put(memberId, new ZKMember(memberId, "", created));
            return this;
        }

        public final Map<String, ZKMember> build() {
            return members;
        }
    }
}
