package com.rtlservice.az.prototype_udp_srv;

import io.vertx.core.Handler;
import io.vertx.core.logging.LoggerFactory;

/**
 *
 * @author Zaitsev Alexei (az) / alexei.zaycev@gmail.com
 */
@FunctionalInterface
public interface HandlerLoggable<E>
    extends Handler<E> {

    @Override
    default void handle(E event) {
        try {
           _handle(event);
        } catch (Throwable ex) {
            LoggerFactory.getLogger(this.getClass()).error(
                    "UNCAUGHT EXCEPTION",
                    ex);
        }
    }

    void _handle(E event);

}