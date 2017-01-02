package de.zalando.paradox.nakadi.consumer.core.http.handlers;

import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.Test;

import de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException;
import de.zalando.paradox.nakadi.consumer.core.http.handlers.testdomain.OrderReceived;

public class EventClassProviderTest {

    @Test
    public void testShouldReturnTheGenericType() {

        Assert.assertEquals(new EventClassProvider<OrderReceived>() { }.getEventClass(), OrderReceived.class);
    }

    @Test
    public void testShouldThrowException() {

        final EventClassProvider orderReceivedEventClassProvider = new EventClassProvider() { };

        Assertions.assertThatThrownBy(orderReceivedEventClassProvider::getEventClass)
                  .isInstanceOf(UnrecoverableException.class).hasMessage(
                      "Concrete Class should give the generic type!");
    }
}
