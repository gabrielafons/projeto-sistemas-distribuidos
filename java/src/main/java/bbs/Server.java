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
    static final String NOME = Util.variavel("SERVER_NAME", "capitao-america-java-1");
    static final String BROKER_HOST = Util.variavel("BROKER_HOST", "broker");
    static final String PROXY_HOST = Util.variavel("PROXY_HOST", "proxy");
    static final String REF_HOST = Util.variavel("REF_HOST", "reference");
    static final String DATA_DIR = Util.variavel("DATA_DIR", "/data");
    static final String PEER_PORT = Util.variavel("PEER_PORT", "6000");
    static final String ENDERECO = Util.variavel("ADVERTISE_ADDR", "tcp://" + NOME + ":" + PEER_PORT);
    static final double DESVIO_RELOGIO = Double.parseDouble(Util.variavel("CLOCK_SKEW", "0"));

    static final int HEARTBEAT_A_CADA = 10;
    static final int SINCRONIZA_A_CADA = 15;
    static final long TIMEOUT_REQ = 2000;
    static final long TIMEOUT_SERVIDOR = 1000;
    static final long ESPERA_ANUNCIO = 5000;
    static final long ANTI_ENTROPIA = 20000;
    static final long TIMEOUT_SYNC = 3000;

    static final String TOPICO_SERVIDORES = "servers";
    static final String TOPICO_REPLICA = "replica";
    static final Set<String> RESERVADOS = Set.of(TOPICO_SERVIDORES, TOPICO_REPLICA);

    private final Messenger mensageiro;
    private final PhysicalClock relogio;
    private final Store armazenamento;
    private int rank = 0;
    private long mensagensRecebidas = 0;
    private String coordenador = null;
    private Map<String, ServerInfo> servidores = new LinkedHashMap<>();
    private boolean precisaEleicao = false;
    private long proximaEleicao = 0;
    private long proximaSync = System.currentTimeMillis() + ANTI_ENTROPIA;
    private final Random sorteio = new Random();

    private final ZContext contexto = new ZContext();
    private final ZMQ.Socket rep;
    private final ZMQ.Socket par;
    private final ZMQ.Socket pub;
    private final ZMQ.Socket sub;

    Server() {
        relogio = new PhysicalClock(DESVIO_RELOGIO);
        mensageiro = new Messenger(NOME, relogio);
        armazenamento = new Store(DATA_DIR, NOME);

        rep = contexto.createSocket(SocketType.REP);
        rep.connect("tcp://" + BROKER_HOST + ":5556");

        par = contexto.createSocket(SocketType.REP);
        par.bind("tcp://*:" + PEER_PORT);

        pub = contexto.createSocket(SocketType.PUB);
        pub.connect("tcp://" + PROXY_HOST + ":5557");

        sub = contexto.createSocket(SocketType.SUB);
        sub.connect("tcp://" + PROXY_HOST + ":5558");
        sub.subscribe(TOPICO_SERVIDORES.getBytes(StandardCharsets.UTF_8));
        sub.subscribe(TOPICO_REPLICA.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] bytesDe(String texto) {
        return texto.getBytes(StandardCharsets.UTF_8);
    }

    private void publicar(String topico, Envelope envelope) {
        pub.send(bytesDe(topico), ZMQ.SNDMORE);
        pub.send(envelope.toByteArray(), 0);
    }

    private Envelope pedir(String endereco, Envelope envelope, long timeoutMs) {
        ZMQ.Socket req = contexto.createSocket(SocketType.REQ);
        try {
            req.setLinger(0);
            req.setSendTimeOut((int) timeoutMs);
            req.connect(endereco);
            if (!req.send(envelope.toByteArray(), 0)) {
                System.out.println("[" + NOME + "] sem resposta de " + endereco + " em " + timeoutMs / 1000.0 + "s");
                return null;
            }
            String destino = endereco.contains("//") ? endereco.split("//", 2)[1] : endereco;
            mensageiro.exibir("ENVIA", envelope, destino, "");
            long fim = System.currentTimeMillis() + timeoutMs;
            while (true) {
                byte[] pedidoPar = par.recv(ZMQ.DONTWAIT);
                if (pedidoPar != null) {
                    atenderPar(pedidoPar);
                }
                byte[] resposta = req.recv(ZMQ.DONTWAIT);
                if (resposta != null) {
                    Envelope recebida = mensageiro.ler(resposta);
                    mensageiro.exibir("RECEBE", recebida, recebida.getRemetente(), "");
                    return recebida;
                }
                if (System.currentTimeMillis() >= fim) {
                    System.out.println("[" + NOME + "] sem resposta de " + endereco + " em " + timeoutMs / 1000.0 + "s");
                    return null;
                }
                Util.dormir(2);
            }
        } catch (InvalidProtocolBufferException e) {
            return null;
        } catch (RuntimeException e) {
            System.out.println("[" + NOME + "] falha ao falar com " + endereco + ": " + e.getMessage());
            return null;
        } finally {
            contexto.destroySocket(req);
        }
    }

    private Envelope referencia(Envelope envelope) {
        return pedir("tcp://" + REF_HOST + ":5559", envelope, TIMEOUT_REQ);
    }

    private void registrar() {
        while (true) {
            Envelope resposta = referencia(mensageiro.montar(Type.OBTER_RANK).setEndereco(ENDERECO).build());
            if (resposta != null && resposta.getStatus() == Status.OK) {
                rank = resposta.getRank();
                System.out.println("[" + NOME + "] rank " + rank + ", endereço " + ENDERECO);
                return;
            }
            Util.dormir(2000);
        }
    }

    private void atualizarServidores() {
        Envelope lista = referencia(mensageiro.montar(Type.LISTAR_SERVIDORES).build());
        if (lista != null && lista.getStatus() == Status.OK) {
            servidores = new LinkedHashMap<>();
            List<String> nomes = new ArrayList<>();
            for (ServerInfo s : lista.getServidoresList()) {
                servidores.put(s.getNome(), s);
                nomes.add(s.getNome() + "(rank " + s.getRank() + ")");
            }
            System.out.println("[" + NOME + "] servidores disponíveis: " + String.join(", ", nomes));
        }
    }

    private void batimento() {
        Envelope resposta = referencia(mensageiro.montar(Type.BATIMENTO).setEndereco(ENDERECO).build());
        if (resposta == null) {
            return;
        }
        if (resposta.getStatus() != Status.OK) {
            registrar();
        }
        atualizarServidores();
    }

    private void eleicao() {
        precisaEleicao = false;
        System.out.println("[" + NOME + "] iniciando eleição (rank " + rank + ")");
        atualizarServidores();
        List<ServerInfo> superiores = new ArrayList<>();
        for (ServerInfo s : servidores.values()) {
            if (s.getRank() > rank) {
                superiores.add(s);
            }
        }
        superiores.sort(Comparator.comparingInt(ServerInfo::getRank).reversed());
        for (ServerInfo s : superiores) {
            Envelope resposta = pedir(s.getEndereco(), mensageiro.montar(Type.ELEICAO).setRank(rank).build(), TIMEOUT_SERVIDOR);
            if (resposta != null && resposta.getStatus() == Status.OK) {
                System.out.println("[" + NOME + "] " + s.getNome() + " (rank " + s.getRank() + ") respondeu OK: aguardando o anúncio");
                coordenador = null;
                proximaEleicao = System.currentTimeMillis() + ESPERA_ANUNCIO;
                return;
            }
        }
        virarCoordenador();
    }

    private void virarCoordenador() {
        coordenador = NOME;
        Envelope anuncio = mensageiro.montar(Type.COORDENADOR).setCoordenador(NOME).setRank(rank).build();
        publicar(TOPICO_SERVIDORES, anuncio);
        mensageiro.exibir("ENVIA", anuncio, "#" + TOPICO_SERVIDORES, "");
        System.out.println("[" + NOME + "] eleito coordenador");
    }

    private void sincronizarRelogio() {
        if (coordenador == null) {
            return;
        }
        if (coordenador.equals(NOME)) {
            System.out.println("[" + NOME + "] sou o coordenador: meu relógio é a referência");
            return;
        }
        ServerInfo info = servidores.get(coordenador);
        if (info == null) {
            atualizarServidores();
            info = servidores.get(coordenador);
        }
        Envelope resposta = null;
        double t0 = 0;
        double t1 = 0;
        if (info != null) {
            t0 = relogio.bruto();
            resposta = pedir(info.getEndereco(), mensageiro.montar(Type.RELOGIO).build(), TIMEOUT_SERVIDOR);
            t1 = relogio.bruto();
        }
        if (resposta == null || resposta.getStatus() != Status.OK) {
            System.out.println("[" + NOME + "] coordenador " + coordenador + " indisponível: pedindo eleição");
            coordenador = null;
            precisaEleicao = true;
            return;
        }
        double correcao = relogio.sincronizar(resposta.getHora(), t0, t1);
        System.out.println(String.format("[%s] relógio ajustado pelo coordenador %s: correção de %+.3fs", NOME, coordenador, correcao));
    }

    private Envelope erro(Type tipo, String texto) {
        return mensageiro.montar(tipo).setStatus(Status.ERRO).setErro(texto).build();
    }

    private void atenderPar(byte[] dados) {
        Envelope envelope;
        try {
            envelope = mensageiro.ler(dados);
        } catch (InvalidProtocolBufferException e) {
            par.send(erro(Type.DESCONHECIDO, "mensagem inválida").toByteArray(), 0);
            return;
        }
        mensageiro.exibir("RECEBE", envelope, envelope.getRemetente(), "");
        Envelope resposta;
        switch (envelope.getTipo()) {
            case ELEICAO:
                resposta = mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).setRank(rank).build();
                precisaEleicao = true;
                break;
            case SINCRONIZAR:
                Envelope.Builder b = mensageiro.montar(envelope.getTipo()).setStatus(Status.OK);
                for (Entry e : armazenamento.entradas) {
                    b.addEntradas(e.paraProto());
                }
                resposta = b.build();
                break;
            case RELOGIO:
                if (NOME.equals(coordenador)) {
                    resposta = mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).setHora(relogio.agora()).build();
                } else {
                    resposta = erro(envelope.getTipo(), "este servidor não é o coordenador");
                }
                break;
            default:
                resposta = erro(envelope.getTipo(), "tipo de mensagem desconhecido");
        }
        par.send(resposta.toByteArray(), 0);
        mensageiro.exibir("ENVIA", resposta, envelope.getRemetente(), "");
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
        Envelope envelope;
        try {
            envelope = mensageiro.ler(dados);
        } catch (InvalidProtocolBufferException e) {
            return;
        }
        if (envelope.getRemetente().equals(NOME)) {
            return;
        }
        mensageiro.exibir("RECEBE", envelope, "#" + topico, "");
        if (topico.equals(TOPICO_REPLICA)) {
            aplicarEntradas(envelope.getEntradasList(), envelope.getRemetente());
            return;
        }
        coordenador = envelope.getCoordenador();
        System.out.println("[" + NOME + "] coordenador agora é " + coordenador);
        if (rank != 0 && envelope.getRank() < rank) {
            precisaEleicao = true;
        }
    }

    private void gravar(Entry entrada) {
        if (armazenamento.adicionar(entrada)) {
            replicar(List.of(entrada));
        }
    }

    private void replicar(List<Entry> entradas) {
        Envelope.Builder b = mensageiro.montar(Type.REPLICAR);
        for (Entry e : entradas) {
            b.addEntradas(e.paraProto());
        }
        Envelope mensagem = b.build();
        publicar(TOPICO_REPLICA, mensagem);
        mensageiro.exibir("ENVIA", mensagem, "#" + TOPICO_REPLICA, "");
    }

    private int aplicarEntradas(List<LogEntry> entradas, String origem) {
        int novas = 0;
        for (LogEntry le : entradas) {
            if (armazenamento.adicionar(Entry.deProto(le))) {
                novas++;
            }
        }
        if (novas > 0) {
            System.out.println("[" + NOME + "] replicação: " + novas + " entrada(s) nova(s) recebida(s) de " + origem);
        }
        return novas;
    }

    private void puxarDe(ServerInfo info) {
        Envelope resposta = pedir(info.getEndereco(), mensageiro.montar(Type.SINCRONIZAR).build(), TIMEOUT_SYNC);
        if (resposta != null && resposta.getStatus() == Status.OK) {
            aplicarEntradas(resposta.getEntradasList(), info.getNome());
        }
    }

    private void antiEntropia(boolean todos) {
        proximaSync = System.currentTimeMillis() + ANTI_ENTROPIA;
        atualizarServidores();
        List<ServerInfo> outros = new ArrayList<>();
        for (ServerInfo s : servidores.values()) {
            if (!s.getNome().equals(NOME)) {
                outros.add(s);
            }
        }
        if (!todos && !outros.isEmpty()) {
            outros = List.of(outros.get(sorteio.nextInt(outros.size())));
        }
        for (ServerInfo info : outros) {
            puxarDe(info);
        }
    }

    private Entry evento(String tipo, String usuario, String canal, String texto) {
        return armazenamento.novaEntrada(tipo, relogio.agora(), mensageiro.relogio, usuario, canal, texto);
    }

    private Envelope tratar(Envelope envelope) {
        switch (envelope.getTipo()) {
            case LOGIN: {
                if (!Util.nomeValido(envelope.getUsuario())) {
                    return erro(envelope.getTipo(), "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                }
                gravar(evento("login", envelope.getUsuario(), "", ""));
                return mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).setUsuario(envelope.getUsuario()).build();
            }
            case CRIAR_CANAL: {
                if (!Util.nomeValido(envelope.getCanal())) {
                    return erro(envelope.getTipo(), "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                }
                if (RESERVADOS.contains(envelope.getCanal())) {
                    return erro(envelope.getTipo(), "'" + envelope.getCanal() + "' é um nome reservado");
                }
                if (armazenamento.temCanal(envelope.getCanal())) {
                    return erro(envelope.getTipo(), "o canal '" + envelope.getCanal() + "' já existe");
                }
                gravar(evento("channel", envelope.getRemetente(), envelope.getCanal(), ""));
                return mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).setCanal(envelope.getCanal()).build();
            }
            case PUBLICAR: {
                if (!Util.nomeValido(envelope.getCanal())) {
                    return erro(envelope.getTipo(), "nome de canal inválido");
                }
                if (RESERVADOS.contains(envelope.getCanal())) {
                    return erro(envelope.getTipo(), "'" + envelope.getCanal() + "' é um nome reservado");
                }
                if (envelope.getTexto().isEmpty()) {
                    return erro(envelope.getTipo(), "mensagem vazia");
                }
                Envelope publicacao = mensageiro.montar(Type.PUBLICACAO)
                        .setUsuario(envelope.getRemetente())
                        .setCanal(envelope.getCanal())
                        .setTexto(envelope.getTexto())
                        .build();
                gravar(armazenamento.novaEntrada("message", publicacao.getInstante(), publicacao.getRelogio(),
                        envelope.getRemetente(), envelope.getCanal(), envelope.getTexto()));
                publicar(envelope.getCanal(), publicacao);
                mensageiro.exibir("ENVIA", publicacao, "#" + envelope.getCanal(), "");
                return mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).setCanal(envelope.getCanal()).build();
            }
            case LISTAR_CANAIS:
                return mensageiro.montar(envelope.getTipo()).setStatus(Status.OK).addAllCanais(armazenamento.nomesCanais()).build();
            default:
                return erro(envelope.getTipo(), "tipo de mensagem desconhecido");
        }
    }

    private void atenderCliente(byte[] dados) {
        Envelope envelope;
        try {
            envelope = mensageiro.ler(dados);
        } catch (InvalidProtocolBufferException e) {
            Envelope resposta = erro(Type.DESCONHECIDO, "mensagem inválida (não é um Envelope Protobuf)");
            rep.send(resposta.toByteArray(), 0);
            mensageiro.exibir("ENVIA", resposta, "", "");
            return;
        }
        mensageiro.exibir("RECEBE", envelope, envelope.getRemetente(), "");
        Envelope resposta = tratar(envelope);
        rep.send(resposta.toByteArray(), 0);
        mensageiro.exibir("ENVIA", resposta, envelope.getRemetente(), "");
        mensagensRecebidas++;
        if (mensagensRecebidas % HEARTBEAT_A_CADA == 0) {
            batimento();
        }
        if (mensagensRecebidas % SINCRONIZA_A_CADA == 0) {
            sincronizarRelogio();
        }
    }

    private void executar() {
        System.out.println("[" + NOME + "] iniciado. " + armazenamento.nomesCanais().size() + " canais carregados de " + armazenamento.caminho);
        registrar();
        antiEntropia(true);
        while (true) {
            boolean ativo = false;
            byte[] pedidoCliente = rep.recv(ZMQ.DONTWAIT);
            if (pedidoCliente != null) {
                atenderCliente(pedidoCliente);
                ativo = true;
            }
            byte[] pedidoPar = par.recv(ZMQ.DONTWAIT);
            if (pedidoPar != null) {
                atenderPar(pedidoPar);
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
            if (precisaEleicao || (coordenador == null && System.currentTimeMillis() >= proximaEleicao)) {
                eleicao();
            }
        }
    }

    public static void main(String[] args) {
        new Server().executar();
    }
}
