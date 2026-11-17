package bbs;

import bbs.ChatProto.Envelope;
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
    static final String BROKER_HOST = Util.variavel("BROKER_HOST", "broker");
    static final String PROXY_HOST = Util.variavel("PROXY_HOST", "proxy");
    static final int TIMEOUT_MS = 5000;

    static final String[] FRASES = {
        "bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
        "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
        "o que vocês acham?"
    };

    private final Random sorteio = new Random();
    private final ZContext contexto = new ZContext();
    private final Messenger mensageiro;
    private final ZMQ.Socket assinante;
    private final Set<String> inscritos = new LinkedHashSet<>();
    private ZMQ.Socket requisicao;
    private String nome;

    Client() {
        String bot = System.getenv("BOT_NAME");
        nome = bot != null && !bot.isEmpty() ? bot : nomeAleatorio();
        mensageiro = new Messenger(nome, new PhysicalClock(0));
        conectar();
        assinante = contexto.createSocket(SocketType.SUB);
        assinante.connect("tcp://" + PROXY_HOST + ":5558");
    }

    private String nomeAleatorio() {
        return "java-bot-" + (1000 + sorteio.nextInt(9000));
    }

    private void conectar() {
        if (requisicao != null) {
            requisicao.setLinger(0);
            contexto.destroySocket(requisicao);
        }
        requisicao = contexto.createSocket(SocketType.REQ);
        requisicao.setReceiveTimeOut(TIMEOUT_MS);
        requisicao.connect("tcp://" + BROKER_HOST + ":5555");
    }

    private Envelope requisitar(Envelope envelope) {
        while (true) {
            requisicao.send(envelope.toByteArray(), 0);
            mensageiro.exibir("ENVIA", envelope, "servidor", "");
            byte[] dados = requisicao.recv(0);
            if (dados != null) {
                try {
                    Envelope resposta = mensageiro.ler(dados);
                    mensageiro.exibir("RECEBE", resposta, resposta.getRemetente(), "");
                    return resposta;
                } catch (InvalidProtocolBufferException e) {
                    System.out.println("[" + nome + "] resposta inválida, tentando de novo");
                }
            } else {
                System.out.println("[" + nome + "] sem resposta em " + TIMEOUT_MS + " ms, tentando de novo");
            }
            conectar();
        }
    }

    private void login() {
        while (true) {
            Envelope resposta = requisitar(mensageiro.montar(Type.LOGIN).setUsuario(nome).build());
            if (resposta.getStatus() == Status.OK) {
                return;
            }
            Util.dormir(1000);
            nome = nomeAleatorio();
            mensageiro.nome = nome;
        }
    }

    private List<String> listarCanais() {
        Envelope resposta = requisitar(mensageiro.montar(Type.LISTAR_CANAIS).build());
        return resposta.getStatus() == Status.OK ? new ArrayList<>(resposta.getCanaisList()) : new ArrayList<>();
    }

    private boolean criarCanal(String nomeCanal) {
        Envelope resposta = requisitar(mensageiro.montar(Type.CRIAR_CANAL).setCanal(nomeCanal).build());
        return resposta.getStatus() == Status.OK;
    }

    private void inscrever(String canal) {
        assinante.subscribe(canal.getBytes(StandardCharsets.UTF_8));
        inscritos.add(canal);
        System.out.println("[" + nome + "] inscrito no canal #" + canal + " (" + inscritos.size() + " inscrições)");
    }

    private boolean publicar(String canal, String texto) {
        Envelope resposta = requisitar(mensageiro.montar(Type.PUBLICAR).setCanal(canal).setTexto(texto).build());
        return resposta.getStatus() == Status.OK;
    }

    private void receber(byte[] topicoBytes) {
        byte[] dados = null;
        while (assinante.hasReceiveMore()) {
            dados = assinante.recv(0);
        }
        if (dados == null) {
            return;
        }
        String canal = new String(topicoBytes, StandardCharsets.UTF_8);
        if (!inscritos.contains(canal)) {
            return;
        }
        try {
            Envelope envelope = mensageiro.ler(dados);
            mensageiro.exibir("RECEBE", envelope, "#" + canal, "  | recebida em " + Util.hora());
        } catch (InvalidProtocolBufferException e) {
            System.out.println("[" + nome + "] publicação inválida ignorada");
        }
    }

    private void esperar(long milissegundos) {
        long fim = System.currentTimeMillis() + milissegundos;
        while (true) {
            long resto = fim - System.currentTimeMillis();
            if (resto <= 0) {
                return;
            }
            byte[] topico = assinante.recv(ZMQ.DONTWAIT);
            if (topico != null) {
                receber(topico);
            } else {
                Util.dormir(Math.min(resto, 2));
            }
        }
    }

    private void executar() {
        login();
        while (true) {
            List<String> canais = listarCanais();
            if (canais.size() < 5) {
                criarCanal("canal-" + (1000 + sorteio.nextInt(9000)));
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
                    inscrever(disponiveis.get(sorteio.nextInt(disponiveis.size())));
                }
            }
            if (!canais.isEmpty()) {
                String canal = canais.get(sorteio.nextInt(canais.size()));
                for (int i = 0; i < 10; i++) {
                    publicar(canal, FRASES[sorteio.nextInt(FRASES.length)]);
                    esperar(1000);
                }
            } else {
                esperar(1000);
            }
        }
    }

    public static void main(String[] args) {
        new Client().executar();
    }
}
