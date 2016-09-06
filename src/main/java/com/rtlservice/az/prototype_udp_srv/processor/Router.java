package com.rtlservice.az.prototype_udp_srv.processor;

import com.rtlservice.az.prototype_udp_srv.HandlerLoggable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 *
 * @author Zaitsev Alexei (az) / alexei.zaycev@gmail.com
 */
public class Router<K, I, O>
    extends AbstractVerticle {

    private static final Logger _LOG = LoggerFactory.getLogger(Router.class);

    public static final BiConsumer<Message<?>, Throwable> REPLY_FAIL = (msg, ex) -> {
        if (msg.replyAddress() != null) {
            msg.fail(-1, ex.toString());
        } else {
            _LOG.error("UNCAUGHT EXCEPTION", ex);
        }
    };

    @Nonnull private final String endpointBase;
    @Nonnull private final Function<K, String> endpointMapper;
    @Nonnull private final Function<I, K> mapper;
    @Nonnull private final Function<Message<I>, O> processor;

    @Nullable private volatile MessageConsumer<I> handler = null;

    public Router(
            @Nonnull String endpointBase,
            @Nonnull Function<K, String> endpointMapper,
            @Nonnull Function<I, K> mapper,
            @Nonnull Function<Message<I>, O> processor) {

        this.endpointBase = endpointBase;
        this.endpointMapper = endpointMapper;
        this.processor = processor;
        this.mapper = mapper;
    }

    public @Nonnull String getEndpoint() {
        return endpointBase;
    }

    public void start(
            @Nonnull Future<Void> start)
            throws Exception {

        try {

            _LOG.debug("{0}: try start router: endpoint={1}",
                    deploymentID(),
                    endpointBase);

            Map<K, ParallelProcessor<K, I, O>> processors = new HashMap<>();

            assert handler == null;
            handler = getVertx().eventBus().consumer(
                    endpointBase,
                    (HandlerLoggable<Message<I>>) (in) -> {
                        try {

                            K id = mapper.apply(in.body());

                            ParallelProcessor<K, I, O> pp = processors.get(id);
                            if (pp == null) {

                                pp = new ParallelProcessor<>(
                                        id,
                                        endpointMapper.apply(id),
                                        mapper,
                                        processor);

                                ParallelProcessor<K, I, O> _pp = processors.put(id, pp);
                                assert _pp == null;

                                getVertx().deployVerticle(
                                        processors.get(id),
                                        new DeploymentOptions().setWorker(true));

                                // костыль, жертвуем задержкой при инициализации в пользу простоты кода
                                pp._waitStart();
                            }

                            if (in.replyAddress() != null) {
                                getVertx().eventBus().send(
                                        pp.getEndpoint(),
                                        in.body(),
                                        (HandlerLoggable<AsyncResult<Message<O>>>) (out) -> {
                                            try {
                                                if (out.succeeded()) {
                                                    in.reply(out.result().body());
                                                } else {
                                                    REPLY_FAIL.accept(in, out.cause());
                                                }
                                            } catch (Throwable ex) {
                                                REPLY_FAIL.accept(in, ex);
                                            }
                                        });
                            } else {
                                getVertx().eventBus().send(
                                        pp.getEndpoint(),
                                        in.body());
                            }

                        } catch (Throwable ex) {
                            REPLY_FAIL.accept(in, ex);
                        }
                    });

            start.complete();

            _LOG.info("{0}: router successfully started: endpoint={1}",
                    deploymentID(),
                    endpointBase);

        } catch (Throwable ex) {

            start.fail(ex);

            _LOG.warn("{0}: router starting fail: endpoint={1}",
                    ex,
                    deploymentID(),
                    endpointBase);
        }
    }

    @Override
    public void stop(
            @Nonnull Future<Void> stop)
            throws Exception {

        try {

            _LOG.debug("{0}: try stop router: endpoint={1}",
                    deploymentID(),
                    endpointBase);

            Objects.requireNonNull(handler).unregister((HandlerLoggable<AsyncResult<Void>>) (async) -> {
                if (async.succeeded()) {
                    _LOG.trace("{0}: router handler successfully stopped",
                            deploymentID());
                } else {
                    _LOG.warn("{0}: router handler stopping fail",
                            async.cause(),
                            deploymentID());
                }
            });

            super.stop(stop);

            _LOG.info("{0}: router successfully stopped: endpoint={1}",
                    deploymentID(),
                    endpointBase);

        } catch (Throwable ex) {

            stop.fail(ex);

            _LOG.warn("{0}: router stopping fail: endpoint={1}",
                    ex,
                    deploymentID(),
                    endpointBase);
        }
    }

}