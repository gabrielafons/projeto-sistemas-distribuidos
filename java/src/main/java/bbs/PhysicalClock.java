package bbs;

final class PhysicalClock {
    private final double desvio;
    private double correcao = 0.0;

    PhysicalClock(double desvio) {
        this.desvio = desvio;
    }

    double bruto() {
        return Util.agora() + desvio;
    }

    double agora() {
        return bruto() + correcao;
    }

    double sincronizar(double horaRemota, double t0, double t1) {
        double estimada = horaRemota + (t1 - t0) / 2.0;
        correcao = estimada - t1;
        return correcao;
    }
}
