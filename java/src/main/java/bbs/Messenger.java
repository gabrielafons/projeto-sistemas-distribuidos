package bbs;

import bbs.ChatProto.Envelope;
import bbs.ChatProto.Type;
import com.google.protobuf.InvalidProtocolBufferException;

final class Messenger {
    String nome;
    final PhysicalClock fisico;
    long relogio = 0;

    Messenger(String nome, PhysicalClock fisico) {
        this.nome = nome;
        this.fisico = fisico;
    }

    Envelope.Builder montar(Type tipo) {
        relogio++;
        return Envelope.newBuilder()
                .setTipo(tipo)
                .setRemetente(nome)
                .setInstante(fisico.agora())
                .setRelogio(relogio);
    }

    Envelope ler(byte[] dados) throws InvalidProtocolBufferException {
        Envelope envelope = Envelope.parseFrom(dados);
        relogio = Math.max(relogio, envelope.getRelogio());
        return envelope;
    }

    void exibir(String direcao, Envelope envelope, String outro, String extra) {
        String seta = direcao.equals("ENVIA") ? "->" : "<-";
        String corpo = envelope.toString().replaceAll("\\s+", " ").trim();
        System.out.println(String.format("%s [%s|L=%d] %-7s %s %-14s %s%s",
                Util.hora(), nome, relogio, direcao, seta, outro, corpo, extra));
    }
}
