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

    static String env(String nome, String padrao) {
        String v = System.getenv(nome);
        return v == null || v.isEmpty() ? padrao : v;
    }

    static double agora() {
        Instant i = Instant.now();
        return i.getEpochSecond() + i.getNano() / 1e9;
    }

    static String hora() {
        return LocalTime.now().format(HORA);
    }

    static boolean nomeValido(String s) {
        return NOME.matcher(s).matches();
    }

    static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
