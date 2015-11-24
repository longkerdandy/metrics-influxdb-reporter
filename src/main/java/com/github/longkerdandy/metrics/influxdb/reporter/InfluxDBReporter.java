package com.github.longkerdandy.metrics.influxdb.reporter;

import com.codahale.metrics.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB Reporter
 */
public class InfluxDBReporter extends ScheduledReporter {

    private final InfluxDB influxDB;
    private final String dbName;

    protected InfluxDBReporter(String url, String username, String password, String dbName,
                               MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        this.influxDB.createDatabase(dbName);
        this.dbName = dbName;
    }

    protected InfluxDBReporter(String url, String username, String password, String dbName,
                               MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor) {
        super(registry, name, filter, rateUnit, durationUnit, executor);
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        this.influxDB.createDatabase(dbName);
        this.dbName = dbName;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        long timestamp = System.currentTimeMillis();
    }

    public void addGauges(BatchPoints batchPoints, SortedMap<String, Gauge> gauges, long timestamp) {
        gauges.forEach((name, gauge) -> {
            Point point = Point.measurement(name + ".gauge")
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("value", gauge.getValue())
                    .build();
            batchPoints.point(point);
        });
    }

    public void addCounters(BatchPoints batchPoints, SortedMap<String, Counter> counters, long timestamp) {
        counters.forEach((name, counter) -> {
            Point point = Point.measurement(name + ".counter")
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("count", counter.getCount())
                    .build();
            batchPoints.point(point);
        });
    }

    public void addHistograms(BatchPoints batchPoints, SortedMap<String, Histogram> histograms, long timestamp) {
        histograms.forEach((name, histogram) -> {
            Snapshot snapshot = histogram.getSnapshot();
            Point point = Point.measurement(name + ".histogram")
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("count", histogram.getCount())
                    .field("max", snapshot.getMax())
                    .field("mean", snapshot.getMean())
                    .field("min", snapshot.getMin())
                    .field("stddev", snapshot.getStdDev())
                    .field("p50", snapshot.getMedian())
                    .field("p75", snapshot.get75thPercentile())
                    .field("p95", snapshot.get95thPercentile())
                    .field("p98", snapshot.get98thPercentile())
                    .field("p99", snapshot.get99thPercentile())
                    .field("p999", snapshot.get999thPercentile())
                    .build();
            batchPoints.point(point);
        });
    }

    protected BatchPoints batchPoints() {
        return BatchPoints
                .database(this.dbName)
                .retentionPolicy("default")
                .consistency(ConsistencyLevel.ALL)
                .build();
    }
}
