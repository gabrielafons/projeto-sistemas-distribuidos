package bbs;

import bbs.ChatProto.EntradaLog;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

final class Armazenamento {
    static final class Entrada {
        @SerializedName("id")
        String id = "";
        @SerializedName("origin")
        String origem = "";
        @SerializedName("kind")
        String tipo = "";
        @SerializedName("ts")
        double instante = 0;
        @SerializedName("clock")
        long relogio = 0;
        @SerializedName("user")
        String usuario = "";
        @SerializedName("channel")
        String canal = "";
        @SerializedName("text")
        String texto = "";

        EntradaLog paraProto() {
            return EntradaLog.newBuilder()
                    .setId(id)
                    .setOrigem(origem)
                    .setTipo(tipo)
                    .setInstante(instante)
                    .setRelogio(relogio)
                    .setUsuario(usuario)
                    .setCanal(canal)
                    .setTexto(texto)
                    .build();
        }

        static Entrada deProto(EntradaLog le) {
            Entrada e = new Entrada();
            e.id = le.getId();
            e.origem = le.getOrigem();
            e.tipo = le.getTipo();
            e.instante = le.getInstante();
            e.relogio = le.getRelogio();
            e.usuario = le.getUsuario();
            e.canal = le.getCanal();
            e.texto = le.getTexto();
            return e;
        }
    }

    private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private final String nomeServidor;
    final Path caminho;
    final List<Entrada> entradas = new ArrayList<>();
    private final Set<String> ids = new HashSet<>();
    private final Set<String> canais = new TreeSet<>();
    private final FileOutputStream arquivo;

    Armazenamento(String pastaDados, String nomeServidor) {
        this.nomeServidor = nomeServidor;
        try {
            Files.createDirectories(Path.of(pastaDados));
            this.caminho = Path.of(pastaDados, nomeServidor + ".jsonl");
            if (Files.exists(caminho)) {
                for (String linha : Files.readAllLines(caminho, StandardCharsets.UTF_8)) {
                    if (!linha.isBlank()) {
                        aplicar(gson.fromJson(linha, Entrada.class));
                    }
                }
            }
            this.arquivo = new FileOutputStream(caminho.toFile(), true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void aplicar(Entrada e) {
        entradas.add(e);
        ids.add(e.id);
        if (e.tipo.equals("channel")) {
            canais.add(e.canal);
        }
    }

    Entrada novaEntrada(String tipo, double instante, long relogio, String usuario, String canal, String texto) {
        Entrada e = new Entrada();
        e.id = UUID.randomUUID().toString().replace("-", "");
        e.origem = nomeServidor;
        e.tipo = tipo;
        e.instante = instante;
        e.relogio = relogio;
        e.usuario = usuario;
        e.canal = canal;
        e.texto = texto;
        return e;
    }

    boolean adicionar(Entrada e) {
        if (ids.contains(e.id)) {
            return false;
        }
        aplicar(e);
        try {
            arquivo.write((gson.toJson(e) + "\n").getBytes(StandardCharsets.UTF_8));
            arquivo.flush();
            arquivo.getFD().sync();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return true;
    }

    boolean temCanal(String nome) {
        return canais.contains(nome);
    }

    List<String> nomesCanais() {
        return new ArrayList<>(canais);
    }
}
