package com.rtlservice.az.prototype_udp_srv.processor;

import com.rtlservice.az.prototype_udp_srv.HandlerLoggable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static com.rtlservice.az.prototype_udp_srv.processor.Router.REPLY_FAIL;

/**
 *
 * @see Router
 *
 * @author Zaitsev Alexei (az) / alexei.zaycev@gmail.com
 */
class ParallelProcessor<K, I, O>
        extends AbstractVerticle {

    private static final Logger _LOG = LoggerFactory.getLogger(ParallelProcessor.class);

    @Nonnull private final K id;
    @Nonnull private final String endpoint;
    @Nonnull private final Function<I, K> mapper;
    @Nonnull private final Function<Message<I>, O> processor;

    private final CountDownLatch init = new CountDownLatch(1);
    @Nullable private volatile MessageConsumer<I> handler = null;

    public ParallelProcessor(
            @Nonnull K id,
            @Nonnull String endpoint,
            @Nonnull Function<I, K> mapper,
            @Nonnull Function<Message<I>, O> processor) {

        this.id = id;
        this.endpoint = endpoint;
        this.processor = processor;
        this.mapper = mapper;
    }

    public @Nonnull String getEndpoint() {
        return endpoint;
    }

    public void start(
            @Nonnull Future<Void> start)
            throws Exception {

        try {

            _LOG.debug("{0}: try start parallel processor: endpoint={1}, id={2}",
                    deploymentID(),
                    endpoint,
                    id.toString());

            assert handler == null;
            handler = getVertx().eventBus().consumer(
                    endpoint,
                    (HandlerLoggable<Message<I>>) (in) -> {
                        try {

                            assert id.equals(mapper.apply(in.body()));

                            O out = processor.apply(in);

                            if (in.replyAddress() != null) {
                                in.reply(out);
                            }

                        } catch (Throwable ex) {
                            REPLY_FAIL.accept(in, ex);
                        }
                    });

            start.complete();

            _LOG.info("{0}: parallel processor successfully started: endpoint={1}, id={2}",
                    deploymentID(),
                    endpoint,
                    id.toString());

        } catch (Throwable ex) {

            start.fail(ex);

            _LOG.warn("{0}: parallel processor starting fail: endpoint={1}, id={2}",
                    ex,
                    deploymentID(),
                    endpoint,
                    id.toString());

        } finally {
            init.countDown();
        }
    }

    @Override
    public void stop(
            @Nonnull Future<Void> stop)
            throws Exception {

        try {

            _LOG.debug("{0}: try stop parallel processor: endpoint={1}, id={2}",
                    deploymentID(),
                    endpoint,
                    id.toString());

            Objects.requireNonNull(handler).unregister((HandlerLoggable<AsyncResult<Void>>) (async) -> {
                if (async.succeeded()) {
                    _LOG.trace("{0}: parallel processor handler successfully stopped",
                            deploymentID());
                } else {
                    _LOG.warn("{0}: parallel processor handler stopping fail",
                            async.cause(),
                            deploymentID());
                }
            });

            super.stop(stop);

            _LOG.info("{0}: parallel processor successfully stopped: endpoint={1}, id={2}",
                    deploymentID(),
                    endpoint,
                    id.toString());

        } catch (Throwable ex) {

            stop.fail(ex);

            _LOG.warn("{0}: parallel processor stopping fail: endpoint={1}, id={2}",
                    ex,
                    deploymentID(),
                    endpoint,
                    id.toString());
        }
    }

    @Deprecated
    void _waitStart()
            throws InterruptedException {

        init.await();
    }

}