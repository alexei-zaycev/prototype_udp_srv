package com.rtlservice.az.prototype_udp_srv;

import com.rtlservice.az.prototype_udp_srv.packet.Packet;
import com.rtlservice.az.prototype_udp_srv.processor.Router;
import com.rtlservice.az.prototype_udp_srv.udp.UDPServer;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.function.BiConsumer;

public class Main {

    private static final Logger _LOG = LoggerFactory.getLogger(Main.class);

    public static void main(
            String[] args) {

        Vertx vertx = Vertx.vertx();

        vertx.eventBus().registerDefaultCodec(
                Packet.class,
                new Packet.Codec());

        Router<Long, Packet, Double> incoming = new Router<>(
                "/udp/incoming/",
                mac -> String.format("/udp/incoming/%012X/", mac),
                packet -> packet.mac,
                packet -> {

                    if (_LOG.isTraceEnabled()) {
                        _LOG.trace("{0}: START PROCESSING: {1}",
                                vertx.getOrCreateContext().deploymentID(),
                                packet.body().toString());
                    }

//                    // emulate processing
//                    try {
//                        Thread.sleep(10000L);
//                    } catch (InterruptedException e) {
//                        // ignore
//                    }

                    _LOG.info("{0}: PROCESSED: {1}",
                            vertx.getOrCreateContext().deploymentID(),
                            packet.body().toString());

                    return Math.random();
                });

        UDPServer udpServer = new UDPServer<>(
                datagram -> {

                    Packet packet = Packet.fromBinary(datagram.data());
                    if (packet == null) {
                        throw new IllegalStateException("Bad packet");
                    }

                    _LOG.info("{0}: RECEIVED: {1}",
                            vertx.getOrCreateContext().deploymentID(),
                            packet.toString());

                    return packet;
                },
                incoming.getEndpoint());

        vertx.deployVerticle(
                incoming,
                async -> {
                    if (async.succeeded()) {
                        vertx.deployVerticle(
                                udpServer);
                    }
                });
    }

}