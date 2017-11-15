package com.zaxxer.influx4j;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Created by brettw on 2017/11/01.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@SuppressWarnings("unused")
public class LineProtocolBench {
   @Param({ "influx4j", "influxdb" })
   private String driver;

   private PointAdapter pointAdapter;

   @Setup(Level.Trial)
   public void createAdapter() {
      switch (driver) {
         case "influx4j":
            pointAdapter = new Influx4jPointAdapter();
            break;
         case "influxdb":
            pointAdapter = new InfluxDbPointAdapter();
            break;
      }
   }

   @Benchmark
   public Object createPointLineProtocol() throws IOException {
      return pointAdapter.createPointLineProtocol();
   }

   /***************************************************************************
    * Internal interfaces
    */

   private interface PointAdapter {
      Object createPointLineProtocol() throws IOException;
   }

   private static class Influx4jPointAdapter implements PointAdapter {
      private static final PointFactory pointFactory = PointFactory.builder()
         .setThreadFactory(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
         })
         .build();

      private final ByteBuffer buffer = ByteBuffer.allocate(256);

      @Override
      public Object createPointLineProtocol() throws IOException {
         final Point point = pointFactory.createPoint("testMeasurement")
                 .tag("zebra", "4")
                 .tag("apple", "1")
                 .tag("table", "3")
                 .tag("mouse", "2")
                 .field("long", 12345)
                 .field("boolean", true)
                 .field("double", 12345.6789d)
                 .field("string", "This is a string");

         point.write(buffer);
         buffer.clear();
         point.release();

        return buffer;
      }
   }

   private static class InfluxDbPointAdapter implements PointAdapter {
      private final ByteBuffer buffer = ByteBuffer.allocate(256);

      @Override
      public Object createPointLineProtocol() throws IOException {
         org.influxdb.dto.Point point = org.influxdb.dto.Point.measurement("testMeasurement")
                 .tag("zebra", "4")
                 .tag("apple", "1")
                 .tag("table", "3")
                 .tag("mouse", "2")
                 .addField("long", 12345)
                 .addField("boolean", true)
                 .addField("double", 12345.6789d)
                 .addField("string", "This is a string")
                 .build();

        buffer.put(point.lineProtocol().getBytes());
        buffer.clear();
        return buffer;
      }
   }
}
