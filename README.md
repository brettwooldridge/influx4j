[![][Build Status img]][Build Status]
[![][license img]][license]
[![][Maven Central img]][Maven Central]
[![][Javadocs img]][Javadocs]

# influx4j

InfluxDB is a high-performance time-series database.  You need a Java driver to match.

When jamming tens-of-thousands of metrics into InfluxDB *per minute*, you can't afford Stop-The-World garbage collection times reaching into the hundreds (or thousands) of milliseconds.  And you can't afford to senselessly burn CPU cycles.

### *10x* Faster.&nbsp;&nbsp;*10x* Less CPU.&nbsp;&nbsp;*Infinity* Less Garbage.

*influx4j is wickedly fast.* 10 times faster than the offical driver.  *influx4j is a CPU miser.*  10 times less CPU consumption persisting a datapoint than the official driver.  *influx4j generates ZERO garbage from Point to Protocol.*  Infinitely less garbage than the official driver.

As measured by the [JMH](http://www.oracle.com/technetwork/articles/java/architect-benchmarking-2266277.html) benchmark, included in this project, comparing *influx4j* with the official driver, Point-to-protocol ...

#### 45 Second run:
| Driver           | Points Produced<br><sup>(approx.)</sup> | Points/ms<br><sup>(approx.)</sup> | Garbage<br>Produced  | Avg Garbage<br>Creation Rate | G1 Garbage<br>Collections |
|:---------------- | ---------------------------------------:| ------:|:--------------------:|:--------------------:|:----------------------:|
| *influx4j*       | 192 million  | 4267 | *zero*   | *zero*       | *zero* |
| *influxdb-java*  | 18 million   |  406 | 334.64 gb | 6.17 gb/sec | 766 |
<br>
Zero garbage means the JVM interrupts your performance critical code less.<sup>1</sup>  The extreme efficiency of the Point-to-protocol buffer serialization pipeline means you burn 10x less CPU producing the same number of points compared to the official driver.

<sub><sup>1</sup>&nbsp;Note: While influx4j generates zero garbage, *your application*, and *associated libraries* likely generate garbage that will still require collection.</sub>

:warning: This driver currently only supports *write-only* operation to InfluxDB, where performance is critical.  I *do* intend to add query support, but the schedule for doing so is currently uncommitted.  The official InfluxDB-java driver may be used in concert with *influx4j* for query execution.

## Usage

### :factory: Creating a ``PointFactory``
Towards the goal of zero-garbage, *influx4j* employs a pooling scheme for ``Point`` instances, such that ``Point`` objects are recycled within the system.  This pool is contained within a factory for producing Points: ``PointFactory``.

The first thing your application will need to do is to configure and create a ``PointFactory``.  The only configuration options are the *initial size* of the pool and the *maximum size* of the pool.

Your application can create multiple ``PointFactory`` instances, or a singleton one; it's up to you.  All methods on the ``PointFactory`` are *thread-safe*, so no additional synchronization is required.

A ``PointFactory`` with a default *initial size* of 128 ``Point`` objects and *maximum size* of 512 ``Point`` objects can be constructed like so:
```Java
PointFactory pointFactory = PointFactory.builder().build();
```
And here is a ``PointFactory`` created with custom configuration:
```Java
PointFactory pointFactory =
   PointFactory.builder()
               .initialSize(1000)
               .maximumSize(8000)
               .build();
```
The ``maximumSize`` should be tuned to somewhat larger than the maximum number of points generated per-second by your application.  That is, assuming the default connection "auto-flush" interval of one second.

The total memory consumed by the pool will be determined by the "high water mark" of usage.  Keep this in mind when setting the ``maximumSize``.  You *can* actually force the pool to empty by calling the ``flush()`` method, but know that doing so will therefore create garbage out of the contents.

#### PointFactory Behaviors
 * Your application will never "block" when creating a ``Point``.  If the internal pool is empty, a new ``Point`` object will be allocated.
 * The internal pool will never exceed the configured maximum size.  If the pool is full when a ``Point`` is returned, that ``Point`` will be discarded for garbage collection.  Therefore, in order to avoid garbage generation, the maximum size should be set based on your application's insertion rate and the configured *auto-flush* rate (*see below*).
 * The internal pool *never shrinks*.  As noted above, you can completely empty the pool by calling the ``flush()`` method on the ``PointFactory`` instance, **but it is not recommended**.
 
You *can* obtain ``Points`` from the ``PointFactory`` that you simply throw away, without damaging the pool.  For example, if your code may throw an exception after creating a ``Point``, but before persisting it, you need not worry about recycling the ``Point`` via try-finally logic etc.  Just don't make a habit of casually throwing away Points, after all, decreasing garbage is one of the goals of the library.

### :diamond_shape_with_a_dot_inside: Creating a ``Point``
Once you have a ``PointFactory`` instance, you are ready to create ``Point`` instances to persist.  The ``Point`` class implements a builder-like pattern.

Example of creating a ``Point`` for a measurement named *"consumerPoll123"*:
```Java
PointFactory pointFactory = ...

Point point = pointFactory
   .createPoint("consumerPoll123")
   .tag("fruit", "apple")
   .field("yummy", true)
   .field("score", 9.5d)
   .timestamp();
```
The *timestamp* can also be specified explicitly:
```Java
Point point = pointFactory
   .createPoint("consumerPoll123")
   .tag("fruit", "banana")
   .field("yummy", false)
   .field("score", 5.0d)
   .timestamp(submissionTS, TimeUnit.MILLISECONDS);
```
Note that while a ``TimeUnit`` may be specified on the ``Point``, the ultimate precision of the persisted timestamp will be determined by the *precision* specified in the connection information (*see below for details about connection parameters*).  The ``TimeUnit`` specified on the ``Point`` timestamp will automatically be converted to the precision of the connection.

``Point`` contains ``field()`` methods for the following Java types: ``String``, ``Long``, ``Double``, ``Boolean``.  *Tag* values, as per InfluxDB specification, must be strings.

#### ``Point`` Copying
It is quite common to have a set of measurements which share a common set of tags, and which are produced at the same time for insertion into InfluxDB.  The ``Point`` class provides a ``copy()`` method that make this more efficient, both in terms of execution time and code brevity.

Copying a ``Point``:
```Java
Point point1 = pointFactory
   .createPoint("procStats")
   .tag("dataCenter", "Tall Pines")
   .tag("hostId", "web.223")
   .field("cpuUsage", hostCpu)
   .field("memTotal", hostMemTotal)
   .field("memFree", hostMemFree)
   .timestamp();

Point point2 = point1
   .copy("netStats")
   .field("inOctets", hostCpu)
   .field("outOctets", hostMemTotal)
```
There are several important things to note about the ``copy()`` method:
 * A new _**measurement**_ name is specified as a parameter to the ``copy()`` method.
 * All _**tags**_ are copied.  In this example, ``point2`` will also contain the *"dataCenter"* and *"hostId"* tags from ``point1``.
 * No _**field**_ values are copied.
 * The _**timestamp**_ of the source ``Point`` is copied (retained).
 * The copied point, ``point2`` in the example above, is a ``Point`` like any other, and therefore additional _**tags**_ and _**fields**_ may be added, and the _**timestamp**_ changed/updated via the standard methods.

### :electric_plug: Connection
An instance of ``InfluxDB`` represents a connection to the database. Similar to the ``PointFactory``, a ``Builder`` is used to configure and create an instance of ``InfluxDB``.

A simple example construction via the ``Builder`` is shown here:
```Java
InfluxDB influxDB = InfluxDB.builder()
         .setConnection("127.0.0.1", 8086, InfluxDB.Protocol.HTTP)
         .setUsername("mueller")
         .setPassword("gotcha")
         .setDatabase("example")
         .build();
```
:point_right: Note that while ``InfluxDB.Protocol.UDP`` is defined, UDP is currently not supported by the driver.

The following configuration parameters are supported by the ``InfluxDB.Builder``:

:cd: ``setDatabase(String database)`` <br>
The name of the InfluxDB database that ``Point`` instances will be inserted into.

:bust_in_silhouette: ``setUsername(String username)`` <br>
The username used to authenticate to the InfluxDB server.

:key: ``setPassword(String password)`` <br>
The password used to authenticate to the InfluxDB server.

:clock3: ``setRetentionPolicy(String retentionPolicy)`` <br>
The name of the retention policy to use.

:loop: ``setConsistency(Consistency consistency)`` <br>
The consistency setting of the connection.  One of:
 * ``InfluxDB.Consistency.ALL``
 * ``InfluxDB.Consistency.ANY``
 * ``InfluxDB.Consistency.ONE``
 * ``InfluxDB.Consistency.QUORUM``.

:stopwatch: ``setPrecision(Precision precision)`` <br>
The precision of timestamps persisted through the connection.  One of:
 * ``InfluxDB.Precision.NANOSECOND``
 * ``InfluxDB.Precision.MICROSECOND``
 * ``InfluxDB.Precision.MILLISECOND``
 * ``InfluxDB.Precision.SECOND``
 * ``InfluxDB.Precision.MINUTE``
 * ``InfluxDB.Precision.HOUR``

:toilet: ``setAutoFlushPeriod(long periodMs)``
The auto-flush period of the connection.  ``Point`` objects that are persisted via the ``write(Point point)`` method, are not written immediately, they are *queued* for writing asynchronously.  The auto-flush period defines how often queued points are written (flushed) to the connection.  The default value is one second (1000ms), and the minimum value is 100ms.

<img src="https://emojipedia-us.s3.amazonaws.com/thumbs/160/emojipedia/132/spool-of-thread_1f9f5.png" height="24px" align="middle"> ``setThreadFactory(ThreadFactory threadFactory)`` <br>
An optional ``ThreadFactory`` used to create the auto-flush background thread.

#### :pencil2: Writing
Writing a ``Point`` is simple, there is only one method: ``write(Point point)``.
```Java
Point point = pointFactory.createPoint("survey")
         .tag("fruit", "apple")
         .field("yummy", true)
         .timestamp();

influxDB.write(point);
```

------------------------------------------------------------------------------------------------------------------------------
See the [InsertionTest](https://github.com/brettwooldridge/influx4j/blob/master/src/test/java/com/zaxxer/influx4j/InsertionTest.java) for example usage, until I have time to write full docs.


[Build Status]:https://travis-ci.org/brettwooldridge/influx4j
[Build Status img]:https://travis-ci.org/brettwooldridge/influx4j.svg?branch=master

[license]:LICENSE
[license img]:https://img.shields.io/badge/license-Apache%202-blue.svg

[Maven Central]:https://maven-badges.herokuapp.com/maven-central/com.zaxxer/influx4j
[Maven Central img]:https://maven-badges.herokuapp.com/maven-central/com.zaxxer/influx4j/badge.svg

[Javadocs]:http://javadoc.io/doc/com.zaxxer/influx4j/1.6
[Javadocs img]:http://javadoc.io/badge/com.zaxxer/influx4j.svg
