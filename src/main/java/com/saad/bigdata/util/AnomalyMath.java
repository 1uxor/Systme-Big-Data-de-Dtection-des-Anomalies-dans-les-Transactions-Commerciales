package com.saad.bigdata.util;

public final class AnomalyMath {
    private AnomalyMath() {}
    public static double amountThreshold(double mean, double std, double multiplier) {
        if (std < 0 || multiplier < 0) throw new IllegalArgumentException("std et multiplicateur positifs requis");
        return mean + multiplier * std;
    }
    public static double periodVariation(long previous, long current) {
        if (previous <= 0) throw new IllegalArgumentException("La période précédente doit être positive");
        return (current - previous) * 100.0 / previous;
    }
}
