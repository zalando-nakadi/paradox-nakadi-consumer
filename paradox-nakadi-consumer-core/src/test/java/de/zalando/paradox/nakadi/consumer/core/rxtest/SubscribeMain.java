package de.zalando.paradox.nakadi.consumer.core.rxtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

//J-
public class SubscribeMain {
    private static final Logger LOG = LoggerFactory.getLogger(SubscribeMain.class);

    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
    private static Scheduler SCHEDULER = Schedulers.io();

    public static void main(final String[] args) throws InterruptedException {
        Observable<Integer> observable = source();
        observable = observable.subscribeOn(SCHEDULER);
        observable = handleSubscription(observable);
        observable = handleRestart(observable);
        Subscription subscription = observable.
                subscribe(next -> LOG.info("Next " + next), e -> LOG.error("Error", e), () -> LOG.info("Completed"));

        Thread.sleep(8000L);
        subscription.unsubscribe();

        LOG.info("Finished");

    }

    private static <T> Observable<T> handleSubscription(final Observable<T> observable) {
        return observable.doOnSubscribe(() -> LOG.info("Handler subscription started"))
                         .doOnUnsubscribe(() -> LOG.info("Handler subscription finished"));
    }

    private static <T> Observable<T> handleRestart(final Observable<T> observable) {
        return observable
                .retryWhen(o -> o.compose(zipWithFlatMap("retry")))
                .repeatWhen(o -> o.compose(zipWithFlatMap("repeat")));
    }

    private static <T> Observable.Transformer<T, Long> zipWithFlatMap(final String reason) {
        return observable ->
                        observable.zipWith(Observable.range(1, Integer.MAX_VALUE),
                                (t, repeatAttempt) -> repeatAttempt)
                                    .flatMap(repeatAttempt -> {
                                        LOG.info("Restart [{}] running [{}] attempt : [{}]", reason, RUNNING.get(), repeatAttempt);
                                        return Observable.timer(2, TimeUnit.SECONDS);
                        }).takeUntil((stopPredicate) -> !RUNNING.get());
    }

    private static Observable<Integer> source() {
        final AtomicInteger counter = new AtomicInteger(0);
        return Observable.defer(() -> {
            final int number = counter.incrementAndGet();
            return Observable.range(1, 5).map(i -> i + 100*number);
        });
    }
}
//J-

