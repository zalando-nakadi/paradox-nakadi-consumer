package de.zalando.paradox.nakadi.consumer.boot;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NakadiHandler {
    String eventName();

    String consumerName() default "";

    boolean consumerNamePostfix() default false;
}
