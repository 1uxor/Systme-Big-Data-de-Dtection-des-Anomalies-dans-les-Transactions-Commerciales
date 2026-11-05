package com.saad.bigdata.util;

import org.junit.jupiter.api.Test;
import com.saad.bigdata.mapreduce.PeriodStatsJob;
import static org.junit.jupiter.api.Assertions.*;

class AnomalyMathTest {
    @Test void amountThreshold() {
        assertEquals(7100,AnomalyMath.amountThreshold(3500,1200,3));
        assertTrue(25000>AnomalyMath.amountThreshold(3500,1200,3));
    }
    @Test void periodVariation() {
        assertEquals(110,AnomalyMath.periodVariation(10000,21000));
        assertEquals(-50,AnomalyMath.periodVariation(100,50));
        assertThrows(IllegalArgumentException.class,()->AnomalyMath.periodVariation(0,10));
    }
    @Test void periodKeys() {
        assertEquals("2026-11-01T10",PeriodStatsJob.period("2026-11-01T10:15:32","hour"));
        assertEquals("2026-11-01",PeriodStatsJob.period("2026-11-01T10:15:32","day"));
        assertEquals("2026-11",PeriodStatsJob.period("2026-11-01T10:15:32","month"));
    }
}
