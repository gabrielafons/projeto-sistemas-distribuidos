package bbs;

import bbs.ChatProto.Envelope;
import bbs.ChatProto.LogEntry;
import bbs.ChatProto.ServerInfo;
import bbs.ChatProto.Status;
import bbs.ChatProto.Type;
import bbs.Store.Entry;
import com.google.protobuf.InvalidProtocolBufferException;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class Server {
    static final String NAME = Util.env("SERVER_NAME", "capitao-america-java-1");
    static final String BROKER_HOST = Util.env("BROKER_HOST", "broker");
    static final String PROXY_HOST = Util.env("PROXY_HOST", "proxy");
    static final String REF_HOST = Util.env("REF_HOST", "reference");
    static final String DATA_DIR = Util.env("DATA_DIR", "/data");
    static final String PEER_PORT = Util.env("PEER_PORT", "6000");
    static final String ADDRESS = Util.env("ADVERTISE_ADDR", "tcp://" + NAME + ":" + PEER_PORT);
    static final double CLOCK_SKEW = Double.parseDouble(Util.env("CLOCK_SKEW", "0"));

    static final int HEARTBEAT_A_CADA = 10;
    static final int SINCRONIZA_A_CADA = 15;
    static final long TIMEOUT_REQ = 2000;
    static final long TIMEOUT_SERVIDOR = 1000;
    static final long ESPERA_ANUNCIO = 5000;
    static final long ANTI_ENTROPIA = 20000;
    static final long TIMEOUT_SYNC = 3000;
    static final int HISTORICO = 10;

    static final String TOPICO_SERVIDORES = "servers";
    static final String TOPICO_REPLICA = "replica";
    static final Set<String> RESERVADOS = Set.of(TOPICO_SERVIDORES, TOPICO_REPLICA);

    private final Messenger msgr;
    private final PhysicalClock relogio;
    private final Store store;
    private int rank = 0;
    private long nMsgs = 0;
    private String coordinator = null;
    private Map<String, ServerInfo> servidores = new LinkedHashMap<>();
    private boolean precisaEleicao = false;
    private long proximaEleicao = 0;
    private long proximaSync = System.currentTimeMillis() + ANTI_ENTROPIA;
    private final Random random = new Random();

    private final ZContext ctx = new ZContext();
    private final ZMQ.Socket rep;
    private final ZMQ.Socket peer;
    private final ZMQ.Socket pub;
    private final ZMQ.Socket sub;

    Server() {
        relogio = new PhysicalClock(CLOCK_SKEW);
        msgr = new Messenger(NAME, relogio);
        store = new Store(DATA_DIR, NAME);

        rep = ctx.createSocket(SocketType.REP);
        rep.connect("tcp://" + BROKER_HOST + ":5556");

        peer = ctx.createSocket(SocketType.REP);
        peer.bind("tcp://*:" + PEER_PORT);

        pub = ctx.createSocket(SocketType.PUB);
        pub.connect("tcp://" + PROXY_HOST + ":5557");

        sub = ctx.createSocket(SocketType.SUB);
        sub.connect("tcp://" + PROXY_HOST + ":5558");
        sub.subscribe(TOPICO_SERVIDORES.getBytes(StandardCharsets.UTF_8));
        sub.subscribe(TOPICO_REPLICA.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private void publicar(String topico, Envelope env) {
        pub.send(bytes(topico), ZMQ.SNDMORE);
        pub.send(env.toByteArray(), 0);
    }

    private Envelope pedir(String endereco, Envelope env, long timeoutMs) {
        ZMQ.Socket s = ctx.createSocket(SocketType.REQ);
        try {
            s.setLinger(0);
            s.setSendTimeOut((int) timeoutMs);
            s.connect(endereco);
            if (!s.send(env.toByteArray(), 0)) {
                System.out.println("[" + NAME + "] sem resposta de " + endereco + " em " + timeoutMs / 1000.0 + "s");
                return null;
            }
            String destino = endereco.contains("//") ? endereco.split("//", 2)[1] : endereco;
            msgr.show("ENVIA", env, destino, "");
            long fim = System.currentTimeMillis() + timeoutMs;
            while (true) {
                byte[] pedidoPeer = peer.recv(ZMQ.DONTWAIT);
                if (pedidoPeer != null) {
                    atenderPeer(pedidoPeer);
                }
                byte[] resposta = s.recv(ZMQ.DONTWAIT);
                if (resposta != null) {
                    Envelope resp = msgr.parse(resposta);
                    msgr.show("RECEBE", resp, resp.getSender(), "");
                    return resp;
                }
                if (System.currentTimeMillis() >= fim) {
                    System.out.println("[" + NAME + "] sem resposta de " + endereco + " em " + timeoutMs / 1000.0 + "s");
                    return null;
                }
                Util.dormir(2);
            }
        } catch (InvalidProtocolBufferException e) {
            return null;
        } catch (RuntimeException e) {
            System.out.println("[" + NAME + "] falha ao falar com " + endereco + ": " + e.getMessage());
            return null;
        } finally {
            ctx.destroySocket(s);
        }
    }

    private Envelope referencia(Envelope env) {
        return pedir("tcp://" + REF_HOST + ":5559", env, TIMEOUT_REQ);
    }

    private void registrar() {
        while (true) {
            Envelope resp = referencia(msgr.build(Type.GET_RANK).setAddress(ADDRESS).build());
            if (resp != null && resp.getStatus() == Status.OK) {
                rank = resp.getRank();
                System.out.println("[" + NAME + "] rank " + rank + ", endereço " + ADDRESS);
                return;
            }
            Util.dormir(2000);
        }
    }

    private void atualizarServidores() {
        Envelope lista = referencia(msgr.build(Type.LIST_SERVERS).build());
        if (lista != null && lista.getStatus() == Status.OK) {
            servidores = new LinkedHashMap<>();
            List<String> nomes = new ArrayList<>();
            for (ServerInfo s : lista.getServersList()) {
                servidores.put(s.getName(), s);
                nomes.add(s.getName() + "(rank " + s.getRank() + ")");
            }
            System.out.println("[" + NAME + "] servidores disponíveis: " + String.join(", ", nomes));
        }
    }

    private void heartbeat() {
        Envelope resp = referencia(msgr.build(Type.HEARTBEAT).setAddress(ADDRESS).build());
        if (resp == null) {
            return;
        }
        if (resp.getStatus() != Status.OK) {
            registrar();
        }
        atualizarServidores();
    }

    private void eleicao() {
        precisaEleicao = false;
        System.out.println("[" + NAME + "] iniciando eleição (rank " + rank + ")");
        atualizarServidores();
        List<ServerInfo> superiores = new ArrayList<>();
        for (ServerInfo s : servidores.values()) {
            if (s.getRank() > rank) {
                superiores.add(s);
            }
        }
        superiores.sort(Comparator.comparingInt(ServerInfo::getRank).reversed());
        for (ServerInfo s : superiores) {
            Envelope resp = pedir(s.getAddress(), msgr.build(Type.ELECTION).setRank(rank).build(), TIMEOUT_SERVIDOR);
            if (resp != null && resp.getStatus() == Status.OK) {
                System.out.println("[" + NAME + "] " + s.getName() + " (rank " + s.getRank() + ") respondeu OK: aguardando o anúncio");
                coordinator = null;
                proximaEleicao = System.currentTimeMillis() + ESPERA_ANUNCIO;
                return;
            }
        }
        virarCoordenador();
    }

    private void virarCoordenador() {
        coordinator = NAME;
        Envelope anuncio = msgr.build(Type.COORDINATOR).setCoordinator(NAME).setRank(rank).build();
        publicar(TOPICO_SERVIDORES, anuncio);
        msgr.show("ENVIA", anuncio, "#" + TOPICO_SERVIDORES, "");
        System.out.println("[" + NAME + "] eleito coordenador");
    }

    private void sincronizarRelogio() {
        if (coordinator == null) {
            return;
        }
        if (coordinator.equals(NAME)) {
            System.out.println("[" + NAME + "] sou o coordenador: meu relógio é a referência");
            return;
        }
        ServerInfo info = servidores.get(coordinator);
        if (info == null) {
            atualizarServidores();
            info = servidores.get(coordinator);
        }
        Envelope resp = null;
        double t0 = 0;
        double t1 = 0;
        if (info != null) {
            t0 = relogio.raw();
            resp = pedir(info.getAddress(), msgr.build(Type.CLOCK).build(), TIMEOUT_SERVIDOR);
            t1 = relogio.raw();
        }
        if (resp == null || resp.getStatus() != Status.OK) {
            System.out.println("[" + NAME + "] coordenador " + coordinator + " indisponível: pedindo eleição");
            coordinator = null;
            precisaEleicao = true;
            return;
        }
        double offset = relogio.sync(resp.getTime(), t0, t1);
        System.out.println(String.format("[%s] relógio ajustado pelo coordenador %s: correção de %+.3fs", NAME, coordinator, offset));
    }

    private Envelope erro(Type type, String texto) {
        return msgr.build(type).setStatus(Status.ERROR).setError(texto).build();
    }

    private void atenderPeer(byte[] dados) {
        Envelope env;
        try {
            env = msgr.parse(dados);
        } catch (InvalidProtocolBufferException e) {
            peer.send(erro(Type.UNKNOWN, "mensagem inválida").toByteArray(), 0);
            return;
        }
        msgr.show("RECEBE", env, env.getSender(), "");
        Envelope resp;
        switch (env.getType()) {
            case ELECTION:
                resp = msgr.build(env.getType()).setStatus(Status.OK).setRank(rank).build();
                precisaEleicao = true;
                break;
            case SYNC:
                Envelope.Builder b = msgr.build(env.getType()).setStatus(Status.OK);
                for (Entry e : store.entries) {
                    b.addEntries(e.toProto());
                }
                resp = b.build();
                break;
            case CLOCK:
                if (NAME.equals(coordinator)) {
                    resp = msgr.build(env.getType()).setStatus(Status.OK).setTime(relogio.now()).build();
                } else {
                    resp = erro(env.getType(), "este servidor não é o coordenador");
                }
                break;
            default:
                resp = erro(env.getType(), "tipo de mensagem desconhecido");
        }
        peer.send(resp.toByteArray(), 0);
        msgr.show("ENVIA", resp, env.getSender(), "");
    }

    private void receberDoSub(byte[] topicoBytes) {
        byte[] dados = null;
        while (sub.hasReceiveMore()) {
            dados = sub.recv(0);
        }
        if (dados == null) {
            return;
        }
        String topico = new String(topicoBytes, StandardCharsets.UTF_8);
        if (!topico.equals(TOPICO_SERVIDORES) && !topico.equals(TOPICO_REPLICA)) {
            return;
        }
        Envelope env;
        try {
            env = msgr.parse(dados);
        } catch (InvalidProtocolBufferException e) {
            return;
        }
        if (env.getSender().equals(NAME)) {
            return;
        }
        msgr.show("RECEBE", env, "#" + topico, "");
        if (topico.equals(TOPICO_REPLICA)) {
            aplicarEntradas(env.getEntriesList(), env.getSender());
            return;
        }
        coordinator = env.getCoordinator();
        System.out.println("[" + NAME + "] coordenador agora é " + coordinator);
        if (rank != 0 && env.getRank() < rank) {
            precisaEleicao = true;
        }
    }

    private void gravar(Entry entrada) {
        if (store.add(entrada)) {
            replicar(List.of(entrada));
        }
    }

    private void replicar(List<Entry> entradas) {
        Envelope.Builder b = msgr.build(Type.REPLICATE);
        for (Entry e : entradas) {
            b.addEntries(e.toProto());
        }
        Envelope msg = b.build();
        publicar(TOPICO_REPLICA, msg);
        msgr.show("ENVIA", msg, "#" + TOPICO_REPLICA, "");
    }

    private int aplicarEntradas(List<LogEntry> entradas, String origem) {
        int novas = 0;
        for (LogEntry le : entradas) {
            if (store.add(Entry.fromProto(le))) {
                novas++;
            }
        }
        if (novas > 0) {
            System.out.println("[" + NAME + "] replicação: " + novas + " entrada(s) nova(s) recebida(s) de " + origem);
        }
        return novas;
    }

    private void puxarDe(ServerInfo info) {
        Envelope resp = pedir(info.getAddress(), msgr.build(Type.SYNC).build(), TIMEOUT_SYNC);
        if (resp != null && resp.getStatus() == Status.OK) {
            aplicarEntradas(resp.getEntriesList(), info.getName());
        }
    }

    private void antiEntropia(boolean todos) {
        proximaSync = System.currentTimeMillis() + ANTI_ENTROPIA;
        atualizarServidores();
        List<ServerInfo> outros = new ArrayList<>();
        for (ServerInfo s : servidores.values()) {
            if (!s.getName().equals(NAME)) {
                outros.add(s);
            }
        }
        if (!todos && !outros.isEmpty()) {
            outros = List.of(outros.get(random.nextInt(outros.size())));
        }
        for (ServerInfo info : outros) {
            puxarDe(info);
        }
    }

    private Entry evento(String kind, String user, String channel, String text) {
        return store.newEntry(kind, relogio.now(), msgr.clock, user, channel, text);
    }

    private Envelope tratar(Envelope env) {
        switch (env.getType()) {
            case LOGIN: {
                if (!Util.nomeValido(env.getUser())) {
                    return erro(env.getType(), "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                }
                gravar(evento("login", env.getUser(), "", ""));
                return msgr.build(env.getType()).setStatus(Status.OK).setUser(env.getUser()).build();
            }
            case CREATE_CHANNEL: {
                if (!Util.nomeValido(env.getChannel())) {
                    return erro(env.getType(), "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                }
                if (RESERVADOS.contains(env.getChannel())) {
                    return erro(env.getType(), "'" + env.getChannel() + "' é um nome reservado");
                }
                if (store.hasChannel(env.getChannel())) {
                    return erro(env.getType(), "o canal '" + env.getChannel() + "' já existe");
                }
                gravar(evento("channel", env.getSender(), env.getChannel(), ""));
                return msgr.build(env.getType()).setStatus(Status.OK).setChannel(env.getChannel()).build();
            }
            case PUBLISH: {
                if (!Util.nomeValido(env.getChannel())) {
                    return erro(env.getType(), "nome de canal inválido");
                }
                if (RESERVADOS.contains(env.getChannel())) {
                    return erro(env.getType(), "'" + env.getChannel() + "' é um nome reservado");
                }
                if (env.getText().isEmpty()) {
                    return erro(env.getType(), "mensagem vazia");
                }
                Envelope publicacao = msgr.build(Type.PUBLICATION)
                        .setUser(env.getSender())
                        .setChannel(env.getChannel())
                        .setText(env.getText())
                        .build();
                gravar(store.newEntry("message", publicacao.getTimestamp(), publicacao.getClock(),
                        env.getSender(), env.getChannel(), env.getText()));
                publicar(env.getChannel(), publicacao);
                msgr.show("ENVIA", publicacao, "#" + env.getChannel(), "");
                return msgr.build(env.getType()).setStatus(Status.OK).setChannel(env.getChannel()).build();
            }
            case LIST_CHANNELS:
                return msgr.build(env.getType()).setStatus(Status.OK).addAllChannels(store.channelNames()).build();
            case HISTORY: {
                if (!Util.nomeValido(env.getChannel())) {
                    return erro(env.getType(), "nome de canal inválido");
                }
                List<Entry> msgs = store.messages(env.getChannel());
                Envelope.Builder b = msgr.build(env.getType())
                        .setStatus(Status.OK)
                        .setChannel(env.getChannel())
                        .setTotal(msgs.size());
                for (Entry e : msgs.subList(Math.max(0, msgs.size() - HISTORICO), msgs.size())) {
                    b.addEntries(e.toProto());
                }
                return b.build();
            }
            default:
                return erro(env.getType(), "tipo de mensagem desconhecido");
        }
    }

    private void atenderCliente(byte[] dados) {
        Envelope env;
        try {
            env = msgr.parse(dados);
        } catch (InvalidProtocolBufferException e) {
            Envelope resp = erro(Type.UNKNOWN, "mensagem inválida (não é um Envelope Protobuf)");
            rep.send(resp.toByteArray(), 0);
            msgr.show("ENVIA", resp, "", "");
            return;
        }
        msgr.show("RECEBE", env, env.getSender(), "");
        Envelope resp = tratar(env);
        rep.send(resp.toByteArray(), 0);
        msgr.show("ENVIA", resp, env.getSender(), "");
        nMsgs++;
        if (nMsgs % HEARTBEAT_A_CADA == 0) {
            heartbeat();
        }
        if (nMsgs % SINCRONIZA_A_CADA == 0) {
            sincronizarRelogio();
        }
    }

    private void run() {
        System.out.println("[" + NAME + "] iniciado. " + store.channelNames().size() + " canais carregados de " + store.path);
        registrar();
        antiEntropia(true);
        while (true) {
            boolean ativo = false;
            byte[] pedidoCliente = rep.recv(ZMQ.DONTWAIT);
            if (pedidoCliente != null) {
                atenderCliente(pedidoCliente);
                ativo = true;
            }
            byte[] pedidoPeer = peer.recv(ZMQ.DONTWAIT);
            if (pedidoPeer != null) {
                atenderPeer(pedidoPeer);
                ativo = true;
            }
            byte[] publicado = sub.recv(ZMQ.DONTWAIT);
            if (publicado != null) {
                receberDoSub(publicado);
                ativo = true;
            }
            if (!ativo) {
                Util.dormir(2);
            }
            if (System.currentTimeMillis() >= proximaSync) {
                antiEntropia(false);
            }
            if (precisaEleicao || (coordinator == null && System.currentTimeMillis() >= proximaEleicao)) {
                eleicao();
            }
        }
    }

    public static void main(String[] args) {
        new Server().run();
    }
}
