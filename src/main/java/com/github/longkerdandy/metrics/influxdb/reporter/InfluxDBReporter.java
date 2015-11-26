package com.github.longkerdandy.metrics.influxdb.reporter;

import com.codahale.metrics.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB Reporter
 */
@SuppressWarnings("unused")
public class InfluxDBReporter extends ScheduledReporter {

    private final InfluxDB influxDB;
    private final String dbName;
    private final Map<String, String> dbTags;
    private final boolean dbBatch;
    private final Clock clock;

    protected InfluxDBReporter(InfluxDB influxDB, String dbName, Map<String, String> dbTags, boolean dbBatch,
                               Clock clock, MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
        this.influxDB = influxDB;
        this.dbName = dbName;
        this.dbTags = dbTags;
        this.dbBatch = dbBatch;
        this.clock = clock;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        long timestamp = this.clock.getTime();
        List<Point> gaugePoints = getGaugePoints(gauges, timestamp);
        List<Point> counterPoints = getCounterPoints(counters, timestamp);
        List<Point> histogramPoints = getHistogramPoints(histograms, timestamp);
        List<Point> meterPoints = getMeterPoints(meters, timestamp);
        List<Point> timerPoints = getTimerPoints(timers, timestamp);
        if (this.dbBatch) {
            BatchPoints batchPoints = BatchPoints.database(this.dbName)
                    .retentionPolicy("default")
                    .consistency(ConsistencyLevel.ALL)
                    .build();
            gaugePoints.forEach(batchPoints::point);
            counterPoints.forEach(batchPoints::point);
            histogramPoints.forEach(batchPoints::point);
            meterPoints.forEach(batchPoints::point);
            timerPoints.forEach(batchPoints::point);
            this.influxDB.write(batchPoints);
        } else {
            gaugePoints.forEach(point -> this.influxDB.write(this.dbName, "default", point));
            counterPoints.forEach(point -> this.influxDB.write(this.dbName, "default", point));
            histogramPoints.forEach(point -> this.influxDB.write(this.dbName, "default", point));
            meterPoints.forEach(point -> this.influxDB.write(this.dbName, "default", point));
            timerPoints.forEach(point -> this.influxDB.write(this.dbName, "default", point));
        }
    }

    protected List<Point> getGaugePoints(SortedMap<String, Gauge> gauges, long timestamp) {
        List<Point> points = new ArrayList<>();
        gauges.forEach((name, gauge) -> {
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .tag(this.dbTags)
                    .field("value", gauge.getValue())
                    .build();
            points.add(point);
        });
        return points;
    }

    protected List<Point> getCounterPoints(SortedMap<String, Counter> counters, long timestamp) {
        List<Point> points = new ArrayList<>();
        counters.forEach((name, counter) -> {
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .tag(this.dbTags)
                    .field("count", counter.getCount())
                    .build();
            points.add(point);
        });
        return points;
    }

    protected List<Point> getHistogramPoints(SortedMap<String, Histogram> histograms, long timestamp) {
        List<Point> points = new ArrayList<>();
        histograms.forEach((name, histogram) -> {
            Snapshot snapshot = histogram.getSnapshot();
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .tag(this.dbTags)
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
            points.add(point);
        });
        return points;
    }

    protected List<Point> getMeterPoints(SortedMap<String, Meter> meters, long timestamp) {
        List<Point> points = new ArrayList<>();
        meters.forEach((name, meter) -> {
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .tag(this.dbTags)
                    .field("count", meter.getCount())
                    .field("m1_rate", convertRate(meter.getOneMinuteRate()))
                    .field("m5_rate", convertRate(meter.getFiveMinuteRate()))
                    .field("m15_rate", convertRate(meter.getFifteenMinuteRate()))
                    .field("mean_rate", convertRate(meter.getMeanRate()))
                    .build();
            points.add(point);
        });
        return points;
    }

    protected List<Point> getTimerPoints(SortedMap<String, Timer> timers, long timestamp) {
        List<Point> points = new ArrayList<>();
        timers.forEach((name, timer) -> {
            Snapshot snapshot = timer.getSnapshot();
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .tag(this.dbTags)
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
            points.add(point);
        });
        return points;
    }

    /**
     * A builder for {@link InfluxDBReporter} instances. Defaults using the default clock,
     * converting rates to events/second, converting durations to milliseconds, and not filtering metrics.
     * Connect to specific InfluxDB with database name 'metrics' in batch mode.
     */
    public static class Builder {
        private final InfluxDB influxDB;
        private String dbName;
        private Map<String, String> dbTags;
        private boolean dbBatch;
        private final MetricRegistry registry;
        private Clock clock;
        private MetricFilter filter;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;

        public Builder(InfluxDB influxDB, MetricRegistry registry) {
            this.influxDB = influxDB;
            this.dbName = "metrics";
            this.dbTags = Maps.newTreeMap(Ordering.natural());
            this.dbBatch = true;
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * InfluxDB database name
         *
         * @param dbName InfluxDB database name
         * @return {@code this}
         */
        public Builder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        /**
         * InfluxDB optional tags
         *
         * @param tags InfluxDB optional dbTags
         * @return {@code this}
         */
        public Builder dbTags(Map<String, String> tags) {
            if (tags != null) this.dbTags.putAll(tags);
            return this;
        }

        /**
         * InfluxDB use batch mode
         *
         * @param dbBatch use batch mode
         * @return {@code this}
         */
        public Builder dbBatch(boolean dbBatch) {
            this.dbBatch = dbBatch;
            return this;
        }

        /**
         * Builds a {@link InfluxDBReporter} with the given properties
         *
         * @return a {@link InfluxDBReporter}
         */
        public InfluxDBReporter build() {
            return new InfluxDBReporter(influxDB, dbName, dbTags, dbBatch,
                    clock, registry, filter, rateUnit, durationUnit
            );
        }
    }
}
