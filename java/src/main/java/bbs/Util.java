package bbs;

import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

final class Util {
    private static final Pattern NOME = Pattern.compile("^[A-Za-z0-9_-]{1,32}$");
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private Util() {
    }

    static String variavel(String nome, String padrao) {
        String valor = System.getenv(nome);
        return valor == null || valor.isEmpty() ? padrao : valor;
    }

    static double agora() {
        Instant instante = Instant.now();
        return instante.getEpochSecond() + instante.getNano() / 1e9;
    }

    static String hora() {
        return LocalTime.now().format(HORA);
    }

    static boolean nomeValido(String texto) {
        return NOME.matcher(texto).matches();
    }

    static void dormir(long milissegundos) {
        try {
            Thread.sleep(milissegundos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
