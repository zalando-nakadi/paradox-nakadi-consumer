package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;

public interface EventClassProvider<T> {
    Class<T> getEventClass();

    static JavaType getJavaType(final EventClassProvider delegate, final ObjectMapper jsonMapper) {
        final Type type = new ParameterizedType() {
            public Type getRawType() {
                return NakadiEventBatch.class;
            }

            public Type getOwnerType() {
                return null;
            }

            public Type[] getActualTypeArguments() {
                return new Type[] {delegate.getEventClass()};
            }
        };
        return jsonMapper.getTypeFactory().constructType(type);
    }
}
