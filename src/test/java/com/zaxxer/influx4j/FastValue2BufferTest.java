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

import com.zaxxer.influx4j.util.FastValue2Buffer;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by brettw on 2017/10/30.
 */
public class FastValue2BufferTest
{
   @Test
   public void testLong2Buffer1Digit() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[1];

      FastValue2Buffer.writeLongToBuffer(0, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 1);
      Assert.assertEquals("0", new String(bytes));

      buffer.flip();
      FastValue2Buffer.writeLongToBuffer(9, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 1);
      Assert.assertEquals("9", new String(bytes));
   }

   @Test
   public void testLong2Buffer2Digit() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[2];

      FastValue2Buffer.writeLongToBuffer(10, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 2);
      Assert.assertEquals("10", new String(bytes));

      buffer.flip();
      FastValue2Buffer.writeLongToBuffer(99, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 2);
      Assert.assertEquals("99", new String(bytes));
   }

   @Test
   public void testLong2Buffer3Digit() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[3];

      FastValue2Buffer.writeLongToBuffer(100, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 3);
      Assert.assertEquals("100", new String(bytes));

      buffer.flip();
      FastValue2Buffer.writeLongToBuffer(999, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 3);
      Assert.assertEquals("999", new String(bytes));
   }

   @Test
   public void testLong2BufferMaxDigits() {
      final String maxLong = "9223372036854775807";
      final String minLong = "-9223372036854775808";
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[20];

      FastValue2Buffer.writeLongToBuffer(Long.MAX_VALUE, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 19);
      Assert.assertEquals(maxLong, new String(bytes, 0, 19));

      buffer.clear();
      FastValue2Buffer.writeLongToBuffer(Long.MIN_VALUE, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 20);
      Assert.assertEquals(minLong, new String(bytes, 0, 20));
   }

   @Test
   public void testLongOffsetHandling() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[20];

      buffer.put("influx4j,".getBytes());

      FastValue2Buffer.writeLongToBuffer(123456, buffer);

      buffer.flip();
      buffer.get(bytes, 0, "influx4j,123456".length());
      Assert.assertEquals("influx4j,123456", new String(bytes, 0, 15));
   }

   @Test
   public void testOneMillionRandomLongs() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[21];
      final ThreadLocalRandom tlr = ThreadLocalRandom.current();

      for (int i = 0; i < 1_000_000; i++) {
         final long number = tlr.nextLong();
         FastValue2Buffer.writeLongToBuffer(number, buffer);

         buffer.flip();
         final int length = buffer.remaining();
         buffer.get(bytes, 0, length);

         final String ourString = new String(bytes, 0, length);
         final String javaString = String.valueOf(number);

         if (!javaString.equals(ourString)) {
            for (int j = 0; j < length; j++) {
               System.out.printf("byte[%d] = %d ('%s')\n", j, bytes[j], "" + ((char) bytes[j]));
            }
         }

         Assert.assertEquals(javaString, ourString);
         buffer.clear();
      }
   }

   @Test
   public void testDouble2Buffer1Digit() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[15];

      FastValue2Buffer.writeDoubleToBuffer(0d, buffer);

      buffer.flip();
      buffer.get(bytes, 0, buffer.limit());
      Assert.assertEquals("0", new String(bytes, 0, buffer.limit()));

      buffer.clear();

      FastValue2Buffer.writeDoubleToBuffer(9.0d, buffer);

      buffer.flip();
      buffer.get(bytes, 0, buffer.limit());
      Assert.assertEquals("9", new String(bytes, 0, buffer.limit()));
   }

   @Test
   public void testDouble2Buffer1and1() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[15];

      FastValue2Buffer.writeDoubleToBuffer(9.2d, buffer);

      buffer.flip();
      buffer.get(bytes, 0, buffer.limit());
      Assert.assertEquals(String.valueOf(9.2d), new String(bytes, 0, buffer.limit()));
   }

   @Test
   public void testDouble2BufferTest2() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[15];

      FastValue2Buffer.writeDoubleToBuffer(12345.6789d, buffer);

      buffer.flip();
      buffer.get(bytes, 0, buffer.limit());
      Assert.assertEquals(String.valueOf(12345.6789d), new String(bytes, 0, buffer.limit()));
   }

   @Test
   public void testDouble2BufferTest3() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[25];

      FastValue2Buffer.writeDoubleToBuffer(Double.MIN_VALUE, buffer);

      buffer.flip();
      buffer.get(bytes, 0, buffer.limit());
      Assert.assertTrue(Double.MIN_VALUE == Double.valueOf(new String(bytes, 0, buffer.limit())));
   }

   @Test
   public void testOneMillionRandomDoubles() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[25];
      final ThreadLocalRandom tlr = ThreadLocalRandom.current();

      for (int i = 0; i < 1_000_000; i++) {
         final double number = tlr.nextDouble();
         FastValue2Buffer.writeDoubleToBuffer(number, buffer);

         buffer.flip();
         int length = buffer.remaining();
         buffer.get(bytes, 0, length);
         final String ourStr = new String(bytes, 0, length).toUpperCase();

         final String numberStr = formatDouble(number, ourStr);
         if (!numberStr.equals(ourStr)) {
            System.out.printf("%s != %s, original number %.25f (String.valueOf(%s))\n", numberStr, ourStr, number, String.valueOf(number));
         }

         Assert.assertEquals(numberStr, ourStr);
         buffer.clear();
      }
   }

   private static String formatDouble(final double d, final String templateString) {
      final boolean useScientificNotation = templateString.contains("E");
      if (useScientificNotation) {
         final int leadingDigits = templateString.indexOf('.');
         final int trailingDigits = templateString.substring(leadingDigits + 1, templateString.indexOf('E')).length();

         final StringBuilder sb = new StringBuilder();
         for (int i = 0; i < leadingDigits; i++) {
            sb.append('#');
         }
         sb.append('.');
         for (int i = 0; i < trailingDigits; i++) {
            sb.append('#');
         }
         sb.append("E0");

         final NumberFormat formatter = new DecimalFormat(sb.toString());
         return formatter.format(d);
      }
      else {
         final int trailingDigits = templateString.substring(templateString.indexOf('.') + 1).length();

         return String.format("%." + trailingDigits + "f", d);
      }
   }
}
