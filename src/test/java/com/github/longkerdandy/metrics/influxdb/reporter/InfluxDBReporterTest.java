package com.github.longkerdandy.metrics.influxdb.reporter;

import com.codahale.metrics.*;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.SortedMap;
import java.util.TreeMap;

/**
 * InfluxDBReporter Test
 */
public class InfluxDBReporterTest {

    private final static String IP = "127.0.0.1";
    private final static long INTERVAL = 5000;

    private static MetricRegistry metrics = new MetricRegistry();
    private static InfluxDBReporter reporter;

    @BeforeClass
    public static void init() {
        reporter = InfluxDBReporter.forRegistry(metrics)
                .dbUrl("http://" + IP + ":8086")
                .dbName("metrics_test")
                .build();
    }

    @AfterClass
    public static void destroy() {
        reporter.influxDB.deleteDatabase("metrics_test");
    }

    @Test
    public void gaugesReportTest() throws InterruptedException {
        SortedMap<String, Gauge> gauges = new TreeMap<>();
        gauges.put("metrics.test.gauges.g1", () -> 100);
        gauges.put("metrics.test.gauges.g2", () -> 200);
        reporter.report(gauges, null, null, null, null);

        gauges.put("metrics.test.gauges.g1", () -> 50);
        gauges.put("metrics.test.gauges.g2", () -> 250);
        reporter.report(gauges, null, null, null, null);

        Thread.sleep(INTERVAL);

        Query query = new Query("SELECT * FROM \"metrics.test.gauges.g1\"", "metrics_test");
        QueryResult result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 100;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 50;

        query = new Query("SELECT * FROM \"metrics.test.gauges.g2\"", "metrics_test");
        result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 200;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 250;
    }

    @Test
    public void countersReportTest() throws InterruptedException {
        SortedMap<String, Counter> counters = new TreeMap<>();
        Counter c1 = new Counter();
        c1.inc(100);
        Counter c2 = new Counter();
        c2.inc(200);
        counters.put("metrics.test.counters.c1", c1);
        counters.put("metrics.test.counters.c2", c2);
        reporter.report(null, counters, null, null, null);

        c1.dec(50);
        c2.inc(50);
        reporter.report(null, counters, null, null, null);

        Thread.sleep(INTERVAL);

        Query query = new Query("SELECT * FROM \"metrics.test.counters.c1\"", "metrics_test");
        QueryResult result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 100;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 50;

        query = new Query("SELECT * FROM \"metrics.test.counters.c2\"", "metrics_test");
        result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 200;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 250;
    }

    @Test
    public void histogramsReportTest() throws InterruptedException {
        SortedMap<String, Histogram> histograms = new TreeMap<>();
        Histogram h1 = new Histogram(new UniformReservoir());
        h1.update(100);
        h1.update(50);
        histograms.put("metrics.test.histograms.h1", h1);
        reporter.report(null, null, histograms, null, null);

        h1 = new Histogram(new UniformReservoir());
        h1.update(200);
        h1.update(250);
        histograms.put("metrics.test.histograms.h1", h1);
        reporter.report(null, null, histograms, null, null);

        Thread.sleep(INTERVAL);

        Query query = new Query("SELECT * FROM \"metrics.test.histograms.h1\"", "metrics_test");
        QueryResult result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(3) == 75;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(3) == 225;
    }

    @Test
    public void metersReportTest() throws InterruptedException {
        SortedMap<String, Meter> meters = new TreeMap<>();
        Meter m1 = new Meter();
        m1.mark(100);
        m1.mark(50);
        meters.put("metrics.test.meters.m1", m1);
        reporter.report(null, null, null, meters, null);

        m1 = new Meter();
        m1.mark(200);
        m1.mark(250);
        meters.put("metrics.test.meters.m1", m1);
        reporter.report(null, null, null, meters, null);

        Thread.sleep(INTERVAL);

        Query query = new Query("SELECT * FROM \"metrics.test.meters.m1\"", "metrics_test");
        QueryResult result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 150;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 450;
    }

    @Test
    public void timersReportTest() throws InterruptedException {
        SortedMap<String, Timer> timers = new TreeMap<>();
        Timer t1 = new Timer();
        try (Timer.Context ignored = t1.time()) {
            Thread.sleep(100);
        }
        try (Timer.Context ignored = t1.time()) {
            Thread.sleep(50);
        }
        timers.put("metrics.test.timers.t1", t1);
        reporter.report(null, null, null, null, timers);

        t1 = new Timer();
        try (Timer.Context ignored = t1.time()) {
            Thread.sleep(200);
        }
        try (Timer.Context ignored = t1.time()) {
            Thread.sleep(250);
        }
        timers.put("metrics.test.timers.t1", t1);
        reporter.report(null, null, null, null, timers);

        Thread.sleep(INTERVAL);

        Query query = new Query("SELECT * FROM \"metrics.test.timers.t1\"", "metrics_test");
        QueryResult result = reporter.influxDB.query(query);
        assert result.getResults().size() == 1;
        assert result.getResults().get(0).getSeries().get(0).getValues().size() == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(0).get(1) == 2;
        assert (Double) result.getResults().get(0).getSeries().get(0).getValues().get(1).get(1) == 2;
    }
}
