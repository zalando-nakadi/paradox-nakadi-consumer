package de.zalando.paradox.nakadi.consumer.core.http;

import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

import rx.Observable;
import rx.Scheduler;
import rx.Subscription;

import rx.functions.Action0;
import rx.functions.Action1;

import rx.schedulers.Schedulers;

public class HttpReactiveReceiver implements Closeable {
    private final Logger log;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private Subscription subscription;

    private final HttpReactiveHandler eventHandler;

    private final Scheduler scheduler;

    public HttpReactiveReceiver(final HttpReactiveHandler eventHandler) {
        this.eventHandler = eventHandler;
        this.log = eventHandler.getLogger(this.getClass());
        this.scheduler = Schedulers.io();
    }

    @VisibleForTesting
    HttpReactiveReceiver(final HttpReactiveHandler eventHandler, final Scheduler scheduler) {
        this.eventHandler = eventHandler;
        this.log = eventHandler.getLogger(this.getClass());
        this.scheduler = scheduler;
    }

    public void init() {
        log.info("Starting HTTP event receiver");

        if (!running.compareAndSet(false, true)) {
            log.info("HTTP reactive receiver is already running");
            return;
        }

        eventHandler.init();

        Observable<HttpResponseChunk> responses = eventHandler.createRequest();
        responses = responses.subscribeOn(scheduler);
        responses = responses.unsubscribeOn(scheduler);
        responses = handleSubscription(responses);
        responses = handleRestart(responses);

        final Action1<Throwable> error = t ->
                log.error("Subscription handler error [{}] / [{}] ", t.getClass().getName(), getMessage(t), t);

        final Action0 compete = () -> log.info("Subscription handler completed");

        subscription = responses.subscribe(getAction(), error, compete);
    }

    private <T> Observable<T> handleSubscription(final Observable<T> observable) {
        return observable.doOnSubscribe(() -> {
                             log.debug("Handler subscription started");
                             eventHandler.onStarted();
                         }).doOnUnsubscribe(() -> {
                             log.debug("Handler subscription finished");
                             eventHandler.onFinished();
                         });
    }

    private <T> Observable<T> handleRestart(final Observable<T> observable) {
        return observable.retryWhen(o -> o.compose(zipWithFlatMap("retry"))).repeatWhen(o ->
                    o.compose(zipWithFlatMap("repeat")));
    }

    //J-
    private <T> Observable.Transformer<T, Long> zipWithFlatMap(final String reason) {
        return
            observable ->
                observable.zipWith(
                        Observable.range(1, Integer.MAX_VALUE), (t, repeatAttempt) -> {
                            // Void or Throwable
                            if (t instanceof Throwable) {
                                log.warn("Exception [{}]", getMessage((Throwable)t));
                            }
                            return repeatAttempt;
                        }).flatMap(repeatAttempt -> {
                            final long retryAfterMillis = eventHandler.getRetryAfterMillis();
                            checkArgument(retryAfterMillis > 0, "RetryAfterMillis must be greater than 0");
                            log.debug("Restart after [{}] running [{}] reason [{}] attempt : [{}]", retryAfterMillis,
                                    running.get(), reason, repeatAttempt);
                            return Observable.timer(retryAfterMillis, TimeUnit.MILLISECONDS);
                          }).takeUntil((stopPredicate) -> !running.get());
    }
    //J+

    private Action1<HttpResponseChunk> getAction() {
        return
            (chunk) -> {
            if (running.get()) {
                try {
                    if (chunk.getStatusCode() == 200) {
                        log.trace("Chunk response event [{}]", chunk.getContent());
                        eventHandler.onResponse(chunk.getContent());
                    } else {
                        log.error("Chunk response error [{}] / [{}]", chunk.getStatusCode(), chunk.getContent());
                        eventHandler.onErrorResponse(chunk.getStatusCode(), chunk.getContent());
                    }
                } catch (Throwable t) {
                    log.error("Unexpected handler error [{}]", getMessage(t));
                    ThrowableUtils.throwException(t);
                }
            } else {
                log.error("Receiving payload but not running");
            }
        };
    }

    @Override
    public void close() throws IOException {
        log.info("Stopping HTTP event receiver");

        if (!running.compareAndSet(true, false)) {
            log.debug("HTTP reactive receiver is already stopped");
            return;
        }

        if (null != subscription) {
            try {
                subscription.unsubscribe();
            } finally {
                subscription = null;
            }
        }

        eventHandler.close();
    }

    public boolean isRunning() {
        return running.get();
    }

    public boolean isSubscribed() {
        final Subscription s = subscription;
        return null != s && !s.isUnsubscribed();
    }
}
