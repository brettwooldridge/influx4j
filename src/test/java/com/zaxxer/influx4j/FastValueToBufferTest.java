/*
 * Copyright (C) 2017, Brett Wooldridge
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

/**
 * Created by brettw on 2017/10/30.
 */
public class FastValueToBufferTest {
   @Test
   public void testLong2Buffer1Digit() {
      final ByteBuffer buffer = ByteBuffer.allocate(64);
      final byte[] bytes = new byte[1];

      FastValue2Buffer.writeLongToBuffer(1, buffer);

      buffer.flip();
      buffer.get(bytes, 0, 1);
      Assert.assertEquals("1", new String(bytes));

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
}
