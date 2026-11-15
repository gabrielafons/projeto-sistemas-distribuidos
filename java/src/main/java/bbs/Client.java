package bbs;

import bbs.ChatProto.Envelope;
import bbs.ChatProto.LogEntry;
import bbs.ChatProto.Status;
import bbs.ChatProto.Type;
import com.google.protobuf.InvalidProtocolBufferException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public final class Client {
    static final String BROKER_HOST = Util.env("BROKER_HOST", "broker");
    static final String PROXY_HOST = Util.env("PROXY_HOST", "proxy");
    static final int TIMEOUT_MS = 5000;

    static final String[] FRASES = {
        "bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
        "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
        "o que vocês acham?"
    };

    private final Random random = new Random();
    private final ZContext ctx = new ZContext();
    private final Messenger msgr;
    private final ZMQ.Socket sub;
    private final Set<String> inscritos = new LinkedHashSet<>();
    private ZMQ.Socket req;
    private String name;

    Client() {
        String bot = System.getenv("BOT_NAME");
        name = bot != null && !bot.isEmpty() ? bot : nomeAleatorio();
        msgr = new Messenger(name, new PhysicalClock(0));
        conectar();
        sub = ctx.createSocket(SocketType.SUB);
        sub.connect("tcp://" + PROXY_HOST + ":5558");
    }

    private String nomeAleatorio() {
        return "java-bot-" + (1000 + random.nextInt(9000));
    }

    private void conectar() {
        if (req != null) {
            req.setLinger(0);
            ctx.destroySocket(req);
        }
        req = ctx.createSocket(SocketType.REQ);
        req.setReceiveTimeOut(TIMEOUT_MS);
        req.connect("tcp://" + BROKER_HOST + ":5555");
    }

    private Envelope requisitar(Envelope env) {
        while (true) {
            req.send(env.toByteArray(), 0);
            msgr.show("ENVIA", env, "servidor", "");
            byte[] dados = req.recv(0);
            if (dados != null) {
                try {
                    Envelope resp = msgr.parse(dados);
                    msgr.show("RECEBE", resp, resp.getSender(), "");
                    return resp;
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("[" + name + "] resposta inválida, tentando de novo");
                }
            } else {
                System.out.println("[" + name + "] sem resposta em " + TIMEOUT_MS + " ms, tentando de novo");
            }
            conectar();
        }
    }

    private void login() {
        while (true) {
            Envelope resp = requisitar(msgr.build(Type.LOGIN).setUser(name).build());
            if (resp.getStatus() == Status.OK) {
                return;
            }
            Util.dormir(1000);
            name = nomeAleatorio();
            msgr.name = name;
        }
    }

    private List<String> listarCanais() {
        Envelope resp = requisitar(msgr.build(Type.LIST_CHANNELS).build());
        return resp.getStatus() == Status.OK ? new ArrayList<>(resp.getChannelsList()) : new ArrayList<>();
    }

    private boolean criarCanal(String nome) {
        Envelope resp = requisitar(msgr.build(Type.CREATE_CHANNEL).setChannel(nome).build());
        return resp.getStatus() == Status.OK;
    }

    private void inscrever(String canal) {
        sub.subscribe(canal.getBytes(StandardCharsets.UTF_8));
        inscritos.add(canal);
        System.out.println("[" + name + "] inscrito no canal #" + canal + " (" + inscritos.size() + " inscrições)");
    }

    private boolean publicar(String canal, String texto) {
        Envelope resp = requisitar(msgr.build(Type.PUBLISH).setChannel(canal).setText(texto).build());
        return resp.getStatus() == Status.OK;
    }

    private void historico(String canal) {
        Envelope resp = requisitar(msgr.build(Type.HISTORY).setChannel(canal).build());
        if (resp.getStatus() != Status.OK) {
            return;
        }
        System.out.println("[" + name + "] histórico de #" + canal + ": " + resp.getTotal()
                + " mensagens no total (respondido por " + resp.getSender() + "); últimas "
                + resp.getEntriesCount() + ":");
        for (LogEntry e : resp.getEntriesList()) {
            System.out.println("    [L=" + e.getClock() + "] " + e.getUser() + ": " + e.getText()
                    + "  (recebida por " + e.getOrigin() + ")");
        }
    }

    private void receber(byte[] topicoBytes) {
        byte[] dados = null;
        while (sub.hasReceiveMore()) {
            dados = sub.recv(0);
        }
        if (dados == null) {
            return;
        }
        String canal = new String(topicoBytes, StandardCharsets.UTF_8);
        if (!inscritos.contains(canal)) {
            return;
        }
        try {
            Envelope env = msgr.parse(dados);
            msgr.show("RECEBE", env, "#" + canal, "  | recebida em " + Util.hora());
        } catch (InvalidProtocolBufferException e) {
            System.out.println("[" + name + "] publicação inválida ignorada");
        }
    }

    private void esperar(long milissegundos) {
        long fim = System.currentTimeMillis() + milissegundos;
        while (true) {
            long resto = fim - System.currentTimeMillis();
            if (resto <= 0) {
                return;
            }
            byte[] topico = sub.recv(ZMQ.DONTWAIT);
            if (topico != null) {
                receber(topico);
            } else {
                Util.dormir(Math.min(resto, 2));
            }
        }
    }

    private void run() {
        login();
        while (true) {
            List<String> canais = listarCanais();
            if (canais.size() < 5) {
                criarCanal("canal-" + (1000 + random.nextInt(9000)));
                canais = listarCanais();
            }
            if (inscritos.size() < 3) {
                List<String> disponiveis = new ArrayList<>();
                for (String c : canais) {
                    if (!inscritos.contains(c)) {
                        disponiveis.add(c);
                    }
                }
                if (!disponiveis.isEmpty()) {
                    inscrever(disponiveis.get(random.nextInt(disponiveis.size())));
                }
            }
            if (!canais.isEmpty()) {
                String canal = canais.get(random.nextInt(canais.size()));
                for (int i = 0; i < 10; i++) {
                    publicar(canal, FRASES[random.nextInt(FRASES.length)]);
                    esperar(1000);
                }
                historico(canal);
            } else {
                esperar(1000);
            }
        }
    }

    public static void main(String[] args) {
        new Client().run();
    }
}
