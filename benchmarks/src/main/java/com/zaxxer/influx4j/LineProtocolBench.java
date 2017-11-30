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

import com.zaxxer.influx4j.InfluxDB.Precision;

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
      private static final PointFactory pointFactory = PointFactory.builder().build();

      private final ByteBuffer buffer = ByteBuffer.allocate(256);

      @Override
      public Object createPointLineProtocol() throws IOException {
         final Point point = pointFactory.createPoint("testMeasurement")
                 .tag("zebra", "cafe")
                 .tag("apple", "rare7")
                 .tag("table", "tiger")
                 .tag("mouse", "beer")
                 .field("long", 12345)
                 .field("boolean", true)
                 .field("double", 12345.6789d)
                 .field("string", "This is a string")
                 .timestamp();

         point.write(buffer, Precision.MILLISECOND);
         buffer.clear();
         point.close();

        return buffer;
      }
   }

   private static class InfluxDbPointAdapter implements PointAdapter {
      private final ByteBuffer buffer = ByteBuffer.allocate(256);

      @Override
      public Object createPointLineProtocol() throws IOException {
         org.influxdb.dto.Point point = org.influxdb.dto.Point
               .measurement("testMeasurement")
               .tag("zebra", "cafe")
               .tag("apple", "rare7")
               .tag("table", "tiger")
               .tag("mouse", "beer")
               .addField("long", 12345)
               .addField("boolean", true)
               .addField("double", 12345.6789d)
               .addField("string", "This is a string")
               .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
               .build();

        buffer.put(point.lineProtocol().getBytes());
        buffer.clear();
        return buffer;
      }
   }
}
