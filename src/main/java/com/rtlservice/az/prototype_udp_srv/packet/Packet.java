package com.rtlservice.az.prototype_udp_srv.packet;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.xml.bind.DatatypeConverter;

@Immutable
public class Packet {

    public final long mac;
    public final byte[] data;

    private Packet(
            Buffer buffer) {

        this.mac = buffer.getLong(12) >> 16;
        this.data = buffer.getBytes(18, buffer.length());
    }

    public static @Nullable Packet fromBinary(
            Buffer buffer) {

        if (buffer.length() <= 18) return null;

        return new Packet(buffer);
    }

    public static @Nonnull Buffer toBinary(
            Packet packet) {

        return toBinary(packet, Buffer.buffer());
    }

    public static @Nonnull Buffer toBinary(
            Packet packet,
            Buffer buffer) {

        buffer.setLong(12, packet.mac << 16);
        buffer.setBytes(18, packet.data);

        return buffer;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("PACKET[")
                .append("header={")
                .append("mac=")
                .append(String.format("%012X", mac))
                .append("} ")
                .append("data=")
                .append(DatatypeConverter.printHexBinary(data))
                .append("]")
                .toString();
    }

    public static class Codec
            implements MessageCodec<Packet, Packet> {

        @Override
        public void encodeToWire(
                Buffer buffer,
                Packet packet) {

            Packet.toBinary(packet, buffer);

        }

        @Override
        public Packet decodeFromWire(
                int pos,
                Buffer buffer) {

            return Packet.fromBinary(
                    buffer.slice(pos, buffer.length()));

        }

        @Override
        public Packet transform(
                Packet packet) {

            return packet;
        }

        @Override
        public String name() {
            return this.getClass().getName();

        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }

}