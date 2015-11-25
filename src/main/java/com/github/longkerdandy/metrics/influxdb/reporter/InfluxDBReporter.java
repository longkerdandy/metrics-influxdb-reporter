package com.github.longkerdandy.metrics.influxdb.reporter;

import com.codahale.metrics.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * InfluxDB Reporter
 */
@SuppressWarnings("unused")
public class InfluxDBReporter extends ScheduledReporter {

    protected final InfluxDB influxDB;
    protected final String dbName;
    protected final Map<String, String> tags;
    protected final Clock clock;

    protected InfluxDBReporter(String url, String username, String password, String dbName, Map<String, String> tags,
                               Clock clock, MetricRegistry registry, MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, "influxdb-reporter", filter, rateUnit, durationUnit);
        this.influxDB = InfluxDBFactory.connect(url, username, password);
        this.influxDB.createDatabase(dbName);
        this.dbName = dbName;
        this.tags = tags;
        this.clock = clock;
    }

    /**
     * Returns a new {@link Builder} for {@link InfluxDBReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link InfluxDBReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        long timestamp = this.clock.getTime();
        BatchPoints batchPoints = batchPoints();
        if (gauges != null) addGauges(batchPoints, gauges, timestamp);
        if (counters != null) addCounters(batchPoints, counters, timestamp);
        if (histograms != null) addHistograms(batchPoints, histograms, timestamp);
        if (meters != null) addMeters(batchPoints, meters, timestamp);
        if (timers != null) addTimers(batchPoints, timers, timestamp);
        this.influxDB.write(batchPoints);
    }

    protected void addGauges(BatchPoints batchPoints, SortedMap<String, Gauge> gauges, long timestamp) {
        gauges.forEach((name, gauge) -> {
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("value", gauge.getValue())
                    .build();
            batchPoints.point(point);
        });
    }

    protected void addCounters(BatchPoints batchPoints, SortedMap<String, Counter> counters, long timestamp) {
        counters.forEach((name, counter) -> {
            Point point = Point.measurement(name)
                    .time(timestamp, TimeUnit.MILLISECONDS)
                    .field("count", counter.getCount())
                    .build();
            batchPoints.point(point);
        });
    }

    protected void addHistograms(BatchPoints batchPoints, SortedMap<String, Histogram> histograms, long timestamp) {
        histograms.forEach((name, histogram) -> {
            Snapshot snapshot = histogram.getSnapshot();
            Point point = Point.measurement(name)
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

    protected void addMeters(BatchPoints batchPoints, SortedMap<String, Meter> meters, long timestamp) {
        meters.forEach((name, meter) -> {
            Point point = Point.measurement(name)
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

    protected void addTimers(BatchPoints batchPoints, SortedMap<String, Timer> timers, long timestamp) {
        timers.forEach((name, timer) -> {
            Snapshot snapshot = timer.getSnapshot();
            Point point = Point.measurement(name)
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

    /**
     * A builder for {@link InfluxDBReporter} instances. Defaults using the default clock,
     * converting rates to events/second, converting durations to milliseconds, and not filtering metrics.
     * Connect to local InfluxDB database 'metrics' with default user name and password
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private MetricFilter filter;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private String url;
        private String username;
        private String password;
        private String dbName;
        private Map<String, String> tags;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.url = "http://127.0.0.1:8086";
            this.username = "root";
            this.password = "root";
            this.dbName = "metrics";
            this.tags = Maps.newTreeMap(Ordering.natural());
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
         * InfluxDB url
         *
         * @param url InfluxDB url
         * @return {@code this}
         */
        public Builder dbUrl(String url) {
            this.url = url;
            return this;
        }

        /**
         * InfluxDB user name
         *
         * @param username InfluxDB user name
         * @return {@code this}
         */
        public Builder dbUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * InfluxDB password
         *
         * @param password InfluxDB password
         * @return {@code this}
         */
        public Builder dbPassword(String password) {
            this.password = password;
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
         * @param tags InfluxDB optional tags
         * @return {@code this}
         */
        public Builder dbTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        /**
         * Builds a {@link InfluxDBReporter} with the given properties
         *
         * @return a {@link InfluxDBReporter}
         */
        public InfluxDBReporter build() {
            return new InfluxDBReporter(url, username, password, dbName, tags,
                    clock, registry, filter, rateUnit, durationUnit
            );
        }
    }
}
