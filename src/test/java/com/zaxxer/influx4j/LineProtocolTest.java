/*
 * Copyright (c) 2017, Brett Wooldridge.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.influx4j;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static java.lang.System.identityHashCode;

/**
 * Created by brettw on 2017/10/31.
 */
public class LineProtocolTest {
   private PointFactory pointFactory;

   @Before
   public void createFactory() {
      pointFactory = PointFactory.builder().build();
   }

   @After
   public void shutdownFactory() {
      pointFactory.close();
   }

   @Test(expected = IllegalStateException.class)
   public void testNoField() throws IOException {
      pointFactory.createPoint("testMeasurement")
            .write(null);
   }

   @Test
   public void testMeasurement() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("boolean", true)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testMeasurementEscaping() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("com,ma")
            .field("boolean", true)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("com\\,ma boolean=t", timestamp), buffer2string(buffer));

      buffer.clear();

      pointFactory.createPoint("sp ace")
            .field("boolean", true)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);
      Assert.assertEquals(tsString("sp\\ ace boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testFieldString() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
           .field("string", "This is a test")
           .timestamp(timestamp, TimeUnit.NANOSECONDS)
           .write(buffer);

      Assert.assertEquals(tsString("testMeasurement string=\"This is a test\"", timestamp), buffer2string(buffer));
   }

   @Test
   public void testStringFieldValueEscaping() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("string", "This \"is\" a test")
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement string=\"This \\\"is\\\" a test\"", timestamp), buffer2string(buffer));
   }


   @Test
   public void testStringFieldKeyEscaping() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("com,ma", 1)
            .field("eq=ual", 2)
            .field("sp ace", 3)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement com\\,ma=1i,eq\\=ual=2i,sp\\ ace=3i", timestamp), buffer2string(buffer));
   }

   @Test
   public void testFieldLong() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("long", 123456)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement long=123456i", timestamp), buffer2string(buffer));
   }

   @Test
   public void testFieldDouble() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("boolean", true)
            .field("double", 123456.789d)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement double=123456.789,boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testMultiFields() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
            .field("string", "This is a test")
            .field("long", Long.MIN_VALUE)
            .field("boolean", true)
            .timestamp(timestamp, TimeUnit.NANOSECONDS)
            .write(buffer);

      Assert.assertEquals(tsString("testMeasurement string=\"This is a test\",long=-9223372036854775808i,boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testTimestamp() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
              .field("boolean", true)
              .timestamp(1509428908609L, TimeUnit.MILLISECONDS)
              .write(buffer);

      Assert.assertEquals("testMeasurement boolean=t 1509428908609000000\n", buffer2string(buffer));
   }

   @Test
   public void testTag() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
              .tag("tag1", "one")
              .field("boolean", true)
              .timestamp(timestamp, TimeUnit.NANOSECONDS)
              .write(buffer);

      Assert.assertEquals(tsString("testMeasurement,tag1=one boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testMultiTags() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
              .tag("tag1", "one")
              .tag("tag2", "two")
              .field("boolean", true)
              .timestamp(timestamp, TimeUnit.NANOSECONDS)
              .write(buffer);

      Assert.assertEquals(tsString("testMeasurement,tag1=one,tag2=two boolean=t", timestamp), buffer2string(buffer));
   }

   @Test
   public void testTagOrdering() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      pointFactory.createPoint("testMeasurement")
              .tag("zebra", "4")
              .tag("apple", "1")
              .tag("table", "3")
              .tag("mouse", "2")
              .field("boolean", true)
              .timestamp(timestamp, TimeUnit.NANOSECONDS)
              .write(buffer);

      Assert.assertEquals(tsString("testMeasurement,apple=1,mouse=2,table=3,zebra=4 boolean=t", timestamp), buffer2string(buffer));
   }

   // @Test
   public void testPointReset() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final PointFactory factory = PointFactory.builder()
              .setSize(1)
              .build();
      try {
         final long timestamp = timestampNs();
         Point point1 = factory.createPoint("testMeasurement")
                 .tag("zebra", "3")
                 .tag("apple", "1")
                 .tag("mouse", "2")
                 .field("boolean", true)
                 .timestamp(timestamp, TimeUnit.NANOSECONDS);

         point1.write(buffer);
         Assert.assertEquals(tsString("testMeasurement,apple=1,mouse=2,zebra=3 boolean=t", timestamp), buffer2string(buffer));

         point1.release();
         buffer.clear();

         Point point2 = factory.createPoint("testMeasurement2")
                 .tag("chocolate", "1")
                 .tag("strawberry", "2")
                 .field("boolean", false)
                 .timestamp(timestamp, TimeUnit.NANOSECONDS);

         Assert.assertEquals(identityHashCode(point1), identityHashCode(point2));

         point2.write(buffer);
         point2.release();

         Assert.assertEquals(tsString("testMeasurement2,chocolate=1,strawberry=2 boolean=f", timestamp), buffer2string(buffer));
      }
      finally {
         factory.close();
      }
   }

   // @Test
   public void testPointMarkRewind() throws IOException {
      final ByteBuffer buffer = ByteBuffer.allocate(128);

      final long timestamp = timestampNs();
      final Point point = pointFactory.createPoint("testMeasurement")
         .tag("table", "3")
         .tag("apple", "1")
         .field("boolean", true)
         .mark()
         .tag("mouse", "2")
         .field("double", 123456789.1234)
         .timestamp(timestamp, TimeUnit.NANOSECONDS);

      point.write(buffer);

      Assert.assertEquals(tsString("testMeasurement,apple=1,mouse=2,table=3 boolean=t,double=123456789.1234", timestamp), buffer2string(buffer));

      buffer.clear();

      point.rewind()
         .tag("zebra", "4")
         .field("double", 987654321.9876)
         .write(buffer);

      Assert.assertEquals(tsString("testMeasurement,apple=1,table=3,zebra=4 boolean=t,double=987654321.9876", timestamp), buffer2string(buffer));

      buffer.clear();

      point.rewind()
         .measurement("testMeasurement2")
         .tag("zebra", "4")
         .field("double", 12121212.1212)
         .write(buffer);

      Assert.assertEquals(tsString("testMeasurement2,apple=1,table=3,zebra=4 boolean=t,double=12121212.1212", timestamp), buffer2string(buffer));

      point.release();
   }

   private String buffer2string(final ByteBuffer buffer) {
      return new String(buffer.array(), 0, buffer.position());
   }

   private static long timestampNs() {
      return TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
   }

   private static String tsString(final String str, final long timestamp) {
      return str + " " + timestamp + "\n";
   }
}
