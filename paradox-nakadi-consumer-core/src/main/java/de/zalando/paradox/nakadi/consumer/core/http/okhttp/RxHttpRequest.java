package de.zalando.paradox.nakadi.consumer.core.http.okhttp;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.exception.ExceptionUtils.getMessage;

import static rx.Observable.using;

import java.net.URL;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

import de.zalando.paradox.nakadi.consumer.core.AuthorizationValueProvider;
import de.zalando.paradox.nakadi.consumer.core.http.HttpGetRequest;
import de.zalando.paradox.nakadi.consumer.core.http.HttpResponseChunk;
import de.zalando.paradox.nakadi.consumer.core.utils.ThrowableUtils;

import okhttp3.Call;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import okio.BufferedSource;

import rx.Observable;

import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

public class RxHttpRequest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RxHttpRequest.class);

    private static final String AUTHORIZATION_HEADER = "Authorization";
    static final String BATCH_SPLITTER = "\n";

    private final OkHttpClient client;
    private final AuthorizationValueProvider authorizationValueProvider;

    public RxHttpRequest(final long readTimeoutMillis,
            @Nullable final AuthorizationValueProvider authorizationValueProvider) {
        this.authorizationValueProvider = authorizationValueProvider;
        this.client = new OkHttpClient.Builder().readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).build();
    }

    private static class HttpCall {
        private final Call call;
        private final Response response;
        private final long threadId;

        HttpCall(final Call call, Response response) {
            this.call = call;
            this.response = response;
            this.threadId = Thread.currentThread().getId();
        }

        void dispose() {
            // must catch exception, otherwise it can be unrecoverable
            if (this.threadId == Thread.currentThread().getId()) {
                if (null != response.body()) {
                    try {
                        // Close on different thread causes java.lang.IllegalStateException: Unbalanced enter/exit
                        response.body().close();
                    } catch (Throwable t) {
                        LOGGER.error("Dispose error request [{}]", call.request(), t);
                    }
                }
            } else {
                try {
                    if (call.isCanceled()) {
                        LOGGER.warn("Already cancelled request [{}]", call.request());
                    } else {
                        LOGGER.info("Cancel request [{}]", call.request());
                        call.cancel();
                    }
                } catch (Throwable t) {
                    LOGGER.error("Cancel error request [{}]", call.request(), t);
                }
            }
        }
    }

    public Observable<HttpResponseChunk> createRequest(final HttpGetRequest requestProducer) {
        final Func0<HttpCall> resourceFactory = () -> {
            Request request = null;
            try {
                request = getRequest(requestProducer.getUrl(), withAuthorization(requestProducer.getHeaders()));
                LOGGER.info("Request [{}]", request);

                final Call call = client.newCall(request);
                final Response response = call.execute();
                LOGGER.debug("Received response with code [{}] and headers [{}]", response.code(), response.headers());
                return new HttpCall(call , response);
            } catch (Throwable t) {
                LOGGER.error("Encountered error while making request [{}] [{}]", request, getMessage(t));
                ThrowableUtils.throwException(t);
                return null;
            }
        };

        //J-
        final Func1<? super HttpCall, ? extends Observable<HttpResponseChunk>> observableFactory = httpCall -> {
            final BufferedSource source = httpCall.response.body().source();
            final Scanner scanner = new Scanner(source.inputStream(), "UTF-8").useDelimiter(BATCH_SPLITTER);
            final Spliterator<String> splt = Spliterators.spliterator(scanner, Long.MAX_VALUE,
                    Spliterator.ORDERED | Spliterator.NONNULL);
            final Stream<String> stream = StreamSupport.stream(splt, false).onClose(scanner::close);
            final int code = httpCall.response.code();
            final Observable<HttpResponseChunk> alternate = code != 200
                ? Observable.just(new HttpResponseChunk(code, "")) : Observable.empty();

            return Observable.from(stream::iterator).map(s -> {
                                 if (isNotEmpty(s)) {
                                     return new HttpResponseChunk(code, s);
                                 } else {
                                     LOGGER.warn("Received empty content");
                                     return new HttpResponseChunk(code, "");
                                 }
                             }).switchIfEmpty(alternate);


        };
        //J+

        final Action1<? super HttpCall> disposeAction = (Action1<HttpCall>) httpCall -> {
            LOGGER.debug("Dispose request [{}]", requestProducer.getUrl());
            httpCall.dispose();
        };

        final AtomicReference<Stopwatch> stopWatchRef = new AtomicReference<>();
        return using(resourceFactory, observableFactory, disposeAction, true).doOnSubscribe(() ->
                    stopWatchRef.set(Stopwatch.createStarted())).doOnTerminate(() -> {
                final Stopwatch stopWatch = stopWatchRef.get();
                LOGGER.info("Processed in [{}] [{}]", (null != stopWatch ? stopWatch.stop().toString() : "?"),
                    requestProducer.getUrl());
            });
    }

    private Map<String, String> withAuthorization(final Map<String, String> headers) {
        if (null == authorizationValueProvider || headers.containsKey(AUTHORIZATION_HEADER)) {
            return headers;
        }

        final Map<String, String> result = new HashMap<>(headers);
        final String value = authorizationValueProvider.get();
        LOGGER.debug("Authorization header value '{}'", value);
        result.put(AUTHORIZATION_HEADER, value);
        return result;
    }

    private static Request getRequest(final URL url, final Map<String, String> headers) {
        return new Request.Builder().url(url).headers(Headers.of(headers)).build();
    }
}
