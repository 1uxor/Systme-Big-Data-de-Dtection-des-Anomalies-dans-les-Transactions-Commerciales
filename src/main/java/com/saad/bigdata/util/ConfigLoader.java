package com.saad.bigdata.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;

/** Valeurs par défaut de l'application, surchargeables par -D et -D Hadoop. */
public final class ConfigLoader {
    private static final Properties VALUES = new Properties();
    static {
        try (InputStream in = ConfigLoader.class.getResourceAsStream("/application.properties")) {
            if (in != null) VALUES.load(in);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    private ConfigLoader() {}
    public static String get(String key, String fallback) {
        String environment = System.getenv(key.toUpperCase(Locale.ROOT).replace('.', '_'));
        String configured = environment != null && !environment.isBlank()
                ? environment : VALUES.getProperty(key, fallback);
        return System.getProperty(key, configured);
    }
    public static void applyAnomalyDefaults(Configuration conf) {
        for (String key : new String[]{"anomaly.std.multiplier", "anomaly.customer.max.transactions",
                "anomaly.customer.max.amount", "anomaly.period.variation.percent", "period.granularity"}) {
            if (conf.get(key) == null) conf.set(key, get(key, ""));
        }
    }
}
