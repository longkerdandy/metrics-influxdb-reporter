### Metrics-InfluxDB-Reporter

DropWizard Metrics Reporter for InfluxDB

[![Build Status](https://travis-ci.org/longkerdandy/metrics-influxdb-reporter.svg?branch=master)](https://travis-ci.org/longkerdandy/metrics-influxdb-reporter)
[![Licnse](https://img.shields.io/badge/License-Apache%20License%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Join the chat at https://gitter.im/longkerdandy/metrics-influxdb-reporter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/longkerdandy/metrics-influxdb-reporter?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

### What is it

[Dropwizard Metrics](http://metrics.dropwizard.io) is a Java library which gives you unparalleled insight into what your code does in production. It provide many metrics method out of box: Gauge, Counter, Histogram, Meter, Timer.

[InfluxDB](https://influxdb.com/) is an open-source distributed time series database with no external dependencies. Could be the new home for all of your metrics, events, and analytics.

So here comes the Metrics-InfluxDB-Reporter, which extends Dropwizard Metrics's ScheduledReporter, publish collected metrics to InfluxDB periodically. Dropwizard Metrics and InfluxDB have some overlapping functions when calculating metrics, I suggest let InfluxDB do the heavy lifting if possible.

### Get started

##### Maven
```
        <dependency>
            <groupId>com.github.longkerdandy</groupId>
            <artifactId>metrics-influxdb-reporter</artifactId>
            <version>1.0.0.Alpha1</version>
        </dependency>
```

##### Gradle
```
        compile 'com.github.longkerdandy:metrics-influxdb-reporter:1.0.0.Alpha1'
```

Using a single batch update to the InfluxDB:
```java
InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086", "root", "root");
String dbName = "metrics";
influxDB.createDatabase(dbName);
InfluxDBReporter reporter = new InfluxDBReporter.Builder(influxDB, new MetricRegistry())
                .dbName(dbName)
                .dbBatch(true)
                .build();
reporter.start(10, TimeUnit.SECONDS);
```

Using preset (or not) batch update to the InfluxDB:
```java
InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086", "root", "root");
String dbName = "metrics";
influxDB.createDatabase(dbName);
influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
InfluxDBReporter reporter = new InfluxDBReporter.Builder(influxDB, new MetricRegistry())
                .dbName(dbName)
                .dbBatch(false)
                .build();
reporter.start(10, TimeUnit.SECONDS);
```

### Build

##### Requirements
* Java 8+
* InfluxDB in localhost (for test only)

##### Build with Gradle Wrapper
Build with tests:
```
>       gradlew clean build
```

Build only, skip tests:
```
>       gradlew clean build -x test
```
