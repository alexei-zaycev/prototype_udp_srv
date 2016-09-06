package com.rtlservice.az.prototype_udp_srv.udp;

import com.rtlservice.az.prototype_udp_srv.HandlerLoggable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.datagram.DatagramPacket;
import io.vertx.core.datagram.DatagramSocket;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 * @author Zaitsev Alexei (az) / alexei.zaycev@gmail.com
 */
public class UDPServer<I, O>
    extends AbstractVerticle {

    private static final Logger _LOG = LoggerFactory.getLogger(UDPServer.class);

    public static final int PORT = 6050;

    @Nonnull private final Function<DatagramPacket, I> decoder;
    @Nonnull private final String incomingProcessorEndpoint;

    @Nullable private volatile DatagramSocket inSocket = null;

    public UDPServer(
            @Nonnull Function<DatagramPacket, I> decoder,
            @Nonnull String incomingProcessorEndpoint) {

        this.decoder = decoder;
        this.incomingProcessorEndpoint = incomingProcessorEndpoint;
    }

    public void start(
            @Nonnull Future<Void> start)
            throws Exception {

        try {

            _LOG.debug("{0}: try start UDP server: port={1,number,#}",
                    deploymentID(),
                    PORT);

            assert inSocket == null;
            inSocket = getVertx().createDatagramSocket().listen(
                    PORT,
                    "localhost",
                    (HandlerLoggable<AsyncResult<DatagramSocket>>) (socket) -> {
                        try {
                            if (socket.succeeded()) {

                                socket.result().handler((HandlerLoggable<DatagramPacket>) (packet) -> {

                                    I in = decoder.apply(packet);

                                    getVertx().eventBus().send(
                                            incomingProcessorEndpoint,
                                            in,
                                            (HandlerLoggable<AsyncResult<Message<Object>>>) (async) -> {
                                                if (async.failed()) {
                                                    _LOG.warn("UNEXPECTED EXCEPTION", async.cause());
                                                }
                                            });
                                });

                                start.complete();

                            } else {
                                start.fail(socket.cause());
                            }
                        } catch (Throwable ex) {
                            start.fail(ex);
                        }
                    });

            _LOG.info("{0}: UDP server successfully started: port={1,number,#}",
                    deploymentID(),
                    PORT);

        } catch (Throwable ex) {

            start.fail(ex);

            _LOG.warn("{0}: UDP server starting fail: port={1,number,#}",
                    ex,
                    deploymentID(),
                    PORT);
        }
    }

    @Override
    public void stop(
            @Nonnull Future<Void> stop)
            throws Exception {

        try {

            _LOG.debug("{0}: try stop UDP server: port={1}",
                    deploymentID(),
                    PORT);

            Objects.requireNonNull(inSocket).close((HandlerLoggable<AsyncResult<Void>>) (async) -> {
                if (async.succeeded()) {
                    _LOG.trace("{0}: UDP server socket successfully closed",
                            deploymentID());
                } else {
                    _LOG.warn("{0}: UDP server socket closing fail",
                            async.cause(),
                            deploymentID());
                }
            });

            super.stop(stop);

            _LOG.info("{0}: UDP server successfully stopped: port={1}",
                    deploymentID(),
                    PORT);

        } catch (Throwable ex) {

            stop.fail(ex);

            _LOG.warn("{0}: UDP server stopping fail: port={1}",
                    ex,
                    deploymentID(),
                    PORT);
        }
    }

}