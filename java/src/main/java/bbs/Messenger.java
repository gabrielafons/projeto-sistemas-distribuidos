package bbs;

import bbs.ChatProto.Envelope;
import bbs.ChatProto.Type;
import com.google.protobuf.InvalidProtocolBufferException;

final class Messenger {
    String name;
    final PhysicalClock physical;
    long clock = 0;

    Messenger(String name, PhysicalClock physical) {
        this.name = name;
        this.physical = physical;
    }

    Envelope.Builder build(Type type) {
        clock++;
        return Envelope.newBuilder()
                .setType(type)
                .setSender(name)
                .setTimestamp(physical.now())
                .setClock(clock);
    }

    Envelope parse(byte[] data) throws InvalidProtocolBufferException {
        Envelope env = Envelope.parseFrom(data);
        clock = Math.max(clock, env.getClock());
        return env;
    }

    void show(String direction, Envelope env, String other, String extra) {
        String seta = direction.equals("ENVIA") ? "->" : "<-";
        String corpo = env.toString().replaceAll("\\s+", " ").trim();
        System.out.println(String.format("%s [%s|L=%d] %-7s %s %-14s %s%s",
                Util.hora(), name, clock, direction, seta, other, corpo, extra));
    }
}
