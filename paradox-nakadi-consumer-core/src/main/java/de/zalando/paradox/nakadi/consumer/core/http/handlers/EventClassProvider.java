package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.Arrays;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.paradox.nakadi.consumer.core.domain.NakadiEventBatch;
import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;

public interface EventClassProvider<T> {

    default Class<T> getEventClass() {
        return Arrays.stream(getClass().getGenericInterfaces()).filter(genericInterface ->
                             genericInterface instanceof ParameterizedType).findFirst().map(genericInterface ->
                             ((ParameterizedType) genericInterface).getActualTypeArguments()).filter(genericTypes ->
                             genericTypes.length > 0).map(genericTypes -> (Class<T>) genericTypes[0]).orElseThrow(
                         () -> new UnrecoverableException("Concrete Class should give the generic type!"));
    }

    static <T> JavaType getJavaType(final EventClassProvider<T> delegate, final ObjectMapper jsonMapper) {
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
