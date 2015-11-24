package com.github.longkerdandy.metrics.influxdb.reporter;

import com.codahale.metrics.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB Reporter
 */
public class InfluxDBReporter extends ScheduledReporter {

    private final InfluxDB influxDB;
    private final String dbName;
    private final Map<String, String> tags;

    protected InfluxDBReporter(String url, String username, String password, String dbName, Map<String, String> tags,
                               MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        this.influxDB.createDatabase(dbName);
        this.dbName = dbName;
        this.tags = tags;
    }

    protected InfluxDBReporter(String url, String username, String password, String dbName, Map<String, String> tags,
                               MetricRegistry registry, String name, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit, ScheduledExecutorService executor) {
        super(registry, name, filter, rateUnit, durationUnit, executor);
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        this.influxDB.createDatabase(dbName);
        this.dbName = dbName;
        this.tags = tags;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        long timestamp = System.currentTimeMillis();
        BatchPoints batchPoints = batchPoints();
        addGauges(batchPoints, gauges, timestamp);
        addCounters(batchPoints, counters, timestamp);
        addHistograms(batchPoints, histograms, timestamp);
        addMeters(batchPoints, meters, timestamp);
        addTimers(batchPoints, timers, timestamp);
        this.influxDB.write(batchPoints);
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
                    .field("size", snapshot.size())
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

    public void addMeters(BatchPoints batchPoints, SortedMap<String, Meter> meters, long timestamp) {
        meters.forEach((name, meter) -> {
            Point point = Point.measurement(name + ".meter")
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("count", meter.getCount())
                    .field("m1_rate", convertRate(meter.getOneMinuteRate()))
                    .field("m5_rate", convertRate(meter.getFiveMinuteRate()))
                    .field("m15_rate", convertRate(meter.getFifteenMinuteRate()))
                    .field("mean_rate", convertRate(meter.getMeanRate()))
                    .build();
            batchPoints.point(point);
        });
    }

    public void addTimers(BatchPoints batchPoints, SortedMap<String, Timer> timers, long timestamp) {
        timers.forEach((name, timer) -> {
            Snapshot snapshot = timer.getSnapshot();
            Point point = Point.measurement(name + ".timer")
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("count", timer.getCount())
                    .field("m1_rate", convertRate(timer.getOneMinuteRate()))
                    .field("m5_rate", convertRate(timer.getFiveMinuteRate()))
                    .field("m15_rate", convertRate(timer.getFifteenMinuteRate()))
                    .field("mean_rate", convertRate(timer.getMeanRate()))
                    .field("size", snapshot.size())
                    .field("max", convertDuration(snapshot.getMax()))
                    .field("mean", convertDuration(snapshot.getMean()))
                    .field("min", convertDuration(snapshot.getMin()))
                    .field("stddev", convertDuration(snapshot.getStdDev()))
                    .field("p50", convertDuration(snapshot.getMedian()))
                    .field("p75", convertDuration(snapshot.get75thPercentile()))
                    .field("p95", convertDuration(snapshot.get95thPercentile()))
                    .field("p98", convertDuration(snapshot.get98thPercentile()))
                    .field("p99", convertDuration(snapshot.get99thPercentile()))
                    .field("p999", convertDuration(snapshot.get999thPercentile()))
                    .build();
            batchPoints.point(point);
        });
    }

    protected BatchPoints batchPoints() {
        BatchPoints.Builder builder = BatchPoints.database(this.dbName)
                .retentionPolicy("default")
                .consistency(ConsistencyLevel.ALL);
        for (Map.Entry<String, String> tag : this.tags.entrySet()) {
            builder = builder.tag(tag.getKey(), tag.getValue());
        }
        return builder.build();
    }
}
