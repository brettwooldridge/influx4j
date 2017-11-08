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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.zaxxer.influx4j.util.DaemonThreadFactory;
import static java.lang.System.identityHashCode;

/**
 * Created by brettw on 2017/10/31.
 */
public class LineProtocolTest {
   private PointFactory pointFactory;

   @Before
   public void createFactory() {
      pointFactory = PointFactory.builder()
            .setThreadFactory(new DaemonThreadFactory())
            .build();
   }

   @After
   public void shutdownFactory() {
      pointFactory.shutdown();
   }

   @Test(expected = IllegalStateException.class)
   public void testNoField() throws IOException {
      pointFactory.createPoint("testMeasurement")
            .writeToStream(null);
   }

   @Test
   public void testMeasurement() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("boolean", true)
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement boolean=t\n", bos.toString());
   }

   @Test
   public void testMeasurementEscaping() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("com,ma")
            .field("boolean", true)
            .writeToStream(bos);
      Assert.assertEquals("com\\,ma boolean=t\n", bos.toString());

      bos.reset();

      pointFactory.createPoint("sp ace")
            .field("boolean", true)
            .writeToStream(bos);
      Assert.assertEquals("sp\\ ace boolean=t\n", bos.toString());
   }

   @Test
   public void testFieldString() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
           .field("string", "This is a test")
           .writeToStream(bos);

      Assert.assertEquals("testMeasurement string=\"This is a test\"\n", bos.toString());
   }

   @Test
   public void testStringFieldValueEscaping() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("string", "This \"is\" a test")
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement string=\"This \\\"is\\\" a test\"\n", bos.toString());
   }


   @Test
   public void testStringFieldKeyEscaping() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("com,ma", 1)
            .field("eq=ual", 2)
            .field("sp ace", 3)
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement com\\,ma=1i,eq\\=ual=2i,sp\\ ace=3i\n", bos.toString());
   }

   @Test
   public void testFieldLong() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("long", 123456)
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement long=123456i\n", bos.toString());
   }

   @Test
   public void testFieldDouble() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("boolean", true)
            .field("double", 123456.789d)
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement boolean=t,double=123456.789\n", bos.toString());
   }

   @Test
   public void testMultiFields() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
            .field("string", "This is a test")
            .field("long", Long.MIN_VALUE)
            .field("boolean", true)
            .writeToStream(bos);

      Assert.assertEquals("testMeasurement string=\"This is a test\",long=-9223372036854775808i,boolean=t\n", bos.toString());
   }

   @Test
   public void testTimestamp() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
              .field("boolean", true)
              .timestamp(1509428908609L, TimeUnit.MILLISECONDS)
              .writeToStream(bos);

      Assert.assertEquals("testMeasurement boolean=t 1509428908609000000\n", bos.toString());
   }

   @Test
   public void testTag() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
              .tag("tag1", "one")
              .field("boolean", true)
              .writeToStream(bos);

      Assert.assertEquals("testMeasurement,tag1=one boolean=t\n", bos.toString());
   }

   @Test
   public void testMultiTags() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
              .tag("tag1", "one")
              .tag("tag2", "two")
              .field("boolean", true)
              .writeToStream(bos);

      Assert.assertEquals("testMeasurement,tag1=one,tag2=two boolean=t\n", bos.toString());
   }

   @Test
   public void testTagOrdering() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      pointFactory.createPoint("testMeasurement")
              .tag("zebra", "4")
              .tag("apple", "1")
              .tag("table", "3")
              .tag("mouse", "2")
              .field("boolean", true)
              .writeToStream(bos);

      Assert.assertEquals("testMeasurement,apple=1,mouse=2,table=3,zebra=4 boolean=t\n", bos.toString());
   }

   @Test
   public void testPointReset() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      final PointFactory factory = PointFactory.builder()
              .setSize(1)
              .build();
      try {
         Point point1 = factory.createPoint("testMeasurement")
                 .tag("zebra", "3")
                 .tag("apple", "1")
                 .tag("mouse", "2")
                 .field("boolean", true);

         point1.writeToStream(bos);
         Assert.assertEquals("testMeasurement,apple=1,mouse=2,zebra=3 boolean=t\n", bos.toString());

         point1.release();
         bos.reset();

         Point point2 = factory.createPoint("testMeasurement2")
                 .tag("chocolate", "1")
                 .tag("strawberry", "2")
                 .field("boolean", false);

         Assert.assertEquals(identityHashCode(point1), identityHashCode(point2));

         point2.writeToStream(bos);
         point2.release();

         Assert.assertEquals("testMeasurement2,chocolate=1,strawberry=2 boolean=f\n", bos.toString());
      }
      finally {
         factory.shutdown();
      }
   }

   // @Test
   public void testPointMarkRewind() throws IOException {
      final ByteArrayOutputStream bos = new ByteArrayOutputStream();

      final Point point = pointFactory.createPoint("testMeasurement")
         .tag("table", "3")
         .tag("apple", "1")
         .field("boolean", true)
         .mark()
         .tag("mouse", "2")
         .field("double", 123456789.1234)
         .writeToStream(bos);

      Assert.assertEquals("testMeasurement,apple=1,mouse=2,table=3 boolean=t,double=123456789.1234\n", bos.toString());

      bos.reset();

      point.rewind()
         .tag("zebra", "4")
         .field("double", 987654321.9876)
         .writeToStream(bos);

      Assert.assertEquals("testMeasurement,apple=1,table=3,zebra=4 boolean=t,double=987654321.9876\n", bos.toString());

      bos.reset();

      point.rewind()
         .measurement("testMeasurement2")
         .tag("zebra", "4")
         .field("double", 12121212.1212)
         .writeToStream(bos);

      Assert.assertEquals("testMeasurement2,apple=1,table=3,zebra=4 boolean=t,double=12121212.1212\n", bos.toString());

      point.release();
   }
}
