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

package com.zaxxer.influx4j.util;

import java.nio.ByteBuffer;

/**
 * Created by brettw on 2017/10/30.
 */
public class FastValue2Buffer
{
   private static final int[] DE_BRUIJN_TABLE = {
           63,  0, 58,  1, 59, 47, 53,  2,
           60, 39, 48, 27, 54, 33, 42,  3,
           61, 51, 37, 40, 49, 18, 28, 20,
           55, 30, 34, 11, 43, 14, 22,  4,
           62, 57, 46, 52, 38, 26, 32, 41,
           50, 36, 17, 19, 29, 10, 13, 21,
           56, 45, 25, 31, 35, 16,  9, 12,
           44, 24, 15,  8, 23,  7,  6,  5 };

   private static final long[] PowersOf10 = {
           0L,
           10L,
           100L,
           1000L,
           10000L,
           100000L,
           1000000L,
           10000000L,
           100000000L,
           1000000000L,
           10000000000L,
           100000000000L,
           1000000000000L,
           10000000000000L,
           100000000000000L,
           1000000000000000L,
           10000000000000000L,
           100000000000000000L,
           1000000000000000000L };
   //      10000000000000000000L    -- too big for signed long

   private static final int NO_NEGATIVE_OFFSET = 0;
   private static final int NEGATIVE_OFFSET = 1;

   private static final byte[] LONG_MINVALUE_BYTES = String.valueOf(Long.MIN_VALUE).getBytes();

   private FastValue2Buffer() {
   }

   public static void writeLongToBuffer(final long value, final ByteBuffer buffer) {
      final long v;
      final int negOffset;
      if (value >= 0) {
         v = value;
         negOffset = NO_NEGATIVE_OFFSET;
      }
      else if (value == Long.MIN_VALUE) {
         buffer.put(LONG_MINVALUE_BYTES);
         return;
      }
      else {
         v = -value;
         buffer.put((byte) '-');
         negOffset = NEGATIVE_OFFSET;
      }

      final int offset = buffer.position();
      final int len = numberDigits(v);
      final byte[] bytes = buffer.array();

      writeNumber(bytes, len, v, offset);
      buffer.position(buffer.position() + len + negOffset);
   }

   public static void writeDoubleToBuffer(final double value, final ByteBuffer buffer) {
      final double v;
      final int negOffset;
      if (value >= 0) {
         v = value;
         negOffset = NO_NEGATIVE_OFFSET;
      }
      else if (value == Long.MIN_VALUE) {
         buffer.put(LONG_MINVALUE_BYTES);
         return;
      }
      else {
         v = -value;
         buffer.put((byte) '-');
         negOffset = NEGATIVE_OFFSET;
      }

      final int offset = buffer.position();
      final long double64 = Double.doubleToRawLongBits(value);
      final long fraction = double64 & 0xFFFFFFFFFFFFFL;
      final int exponent = (int) (double64 >> 53L) & 0b011111111111;
      switch (exponent) {
         case 0x000:
            // is used to represent a signed zero (if F=0) and subnormals (if F≠0), where F is the
            // fractional part of the significand.  In the case of subnormals (e=0) the double-precision number
            // is described by: (-1 sign) x 2^-1022 x 0.fraction
            break;
         case 0x001:
            // 2^(1-1023) = 2^-1022 (smallest exponent for normal numbers)
            break;
         case 0x7fe:
            // 2^(2046-1023) = 2^1023 (highest exponent)
            break;
         case 0x7ff:
            // is used to represent ∞ (if F=0) and NaNs (if F≠0), where F is the fractional part of the significand.
            break;
         default:
            // Except for the above exceptions, the entire double-precision number is described by:
            // (-1 sign) x 2^(e-1023) x 1.fraction
            break;
      }
   }

   private static void writeNumber(final byte[] buffer, final int len, final long value, final int offset) {
      switch (len) {
         case 1:
            buffer[offset] = (byte) ('0' + value);
            break;
         default:
            writeChar(buffer, len, value, offset);
            break;
      }
   }

   private static void writeChar(final byte[] buffer, final int len, final long value, final int offset) {
      final long div = value / 10L;
      final long rem = value % 10L;
      buffer[len - 1 + offset] = (byte) ('0' + rem);

      writeNumber(buffer, len - 1, div, offset);
   }

   private static int log2(long value) {
      value |= value >> 1;
      value |= value >> 2;
      value |= value >> 4;
      value |= value >> 8;
      value |= value >> 16;
      value |= value >> 32;
      return DE_BRUIJN_TABLE[(int) (((value - (value >> 1)) * 0x07EDD5E59A4E28C2L) >>> 58)];
   }

   private static int numberDigits(long value) {
      final int t = (log2(value) + 1) * 1233 >> 12;
      return 1 + t - (value < PowersOf10[t] ? 1 : 0);
   }
}
