package de.zalando.paradox.nakadi.consumer.partitioned.zk;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;

import org.json.JSONException;
import org.json.JSONObject;

import de.zalando.paradox.nakadi.consumer.core.utils.LocalHostUtils;

public class ZKMember {
    private final String memberId;
    private final String host;
    private final long created;

    ZKMember(final String memberId, final String host, final long created) {
        this.memberId = requireNonNull(memberId, "memberId must not be null");
        this.host = host;
        this.created = created;
    }

    private ZKMember(final String memberId) {
        this(memberId, LocalHostUtils.getHostName(), System.currentTimeMillis());
    }

    static ZKMember of(final String memberId) {
        return new ZKMember(memberId);
    }

    byte[] toByteJson() {
        final JSONObject json = new JSONObject();
        json.put("memberId", memberId);
        json.put("host", host);
        json.put("created", created);
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    static ZKMember fromByteJson(final byte[] data) throws JSONException {
        final JSONObject json = new JSONObject(new String(data, StandardCharsets.UTF_8));
        final String memberId = json.getString("memberId");
        final String host = json.getString("host");
        final long created = json.getLong("created");
        return new ZKMember(memberId, host, created);
    }

    public String getMemberId() {
        return memberId;
    }

    public long getCreated() {
        return created;
    }

    public String toString() {
        return this.memberId + ":" + this.host + ":" + this.created;
    }
}
