package bbs;

final class PhysicalClock {
    private final double skew;
    private double offset = 0.0;

    PhysicalClock(double skew) {
        this.skew = skew;
    }

    double raw() {
        return Util.agora() + skew;
    }

    double now() {
        return raw() + offset;
    }

    double sync(double horaRemota, double t0, double t1) {
        double estimada = horaRemota + (t1 - t0) / 2.0;
        offset = estimada - t1;
        return offset;
    }
}
