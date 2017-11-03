[![][Build Status img]][Build Status]
[![][license img]][license]

# influx4j<sup><sup><sup>&nbsp;(*Work in-progress.  Est. v1.0 by end of Nov. 2017.*)</sup></sup></sup>

InfluxDB is a high-performance time-series database.  You need a Java driver to match.

When jamming tens-of-thousands of metrics into InfluxDB *per minute*, you can't afford Stop-The-World garbage collection times reaching into the hundreds (or thousands) of milliseconds.  And you can't afford to senselessly burn CPU cycles.

### *25x* Faster.&nbsp;&nbsp;*25x* Less CPU.&nbsp;&nbsp;*Infinity* Less Garbage.

*influx4j is wickedly fast.* 25 times faster than the offical driver.  *influx4j is a CPU miser.*  25 times less CPU consumption persisting a datapoint than the official driver.  *influx4j generates ZERO garbage from Point to Protocol.*  Infinitely less garbage than the official driver.

As measured by the [JMH](http://www.oracle.com/technetwork/articles/java/architect-benchmarking-2266277.html) benchmark, included in this project, comparing *influx4j* with the official driver, Point-to-protocol ...

#### 45 Second run:
| Driver           | Points Produced<br><sup>(approx.)</sup> | Points/ms<br><sup>(approx.)</sup> | Garbage<br>Produced  | Avg Garbage<br>Creation Rate | G1 Garbage<br>Collections |
|:---------------- | ---------------------------------------:| ------:|:--------------------:|:--------------------:|:----------------------:|
| *influx4j*       | 279 million  | 6200 | *zero*   | *zero*       | *zero* |
| *influxdb-java*  | 12 million   |  264 | 334.64 gb | 6.17 gb/sec | 766 |
<br>
Zero garbage means the JVM interrupts your performance critical code less.<sup>1</sup>  The extreme efficiency of the Point-to-protocol buffer serialization pipeline means you burn 25x less CPU producing the same number of points compared to the official driver.

<sub><sup>1</sup>&nbsp;Note: While influx4j generates zero garbage, *your application*, and *associated libraries* likely generate garbage that will still require collection.</sub>

[Build Status]:https://travis-ci.org/brettwooldridge/influx4j
[Build Status img]:https://travis-ci.org/brettwooldridge/influx4j.svg?branch=master

[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202-blue.svg
