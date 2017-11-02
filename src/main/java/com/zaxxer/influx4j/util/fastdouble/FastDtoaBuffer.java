/* -*- Mode: java; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 4 -*-
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package com.zaxxer.influx4j.util.fastdouble;

import java.util.Arrays;
import java.nio.ByteBuffer;

public final class FastDtoaBuffer {
   private final DiyFp[] diyFps = new DiyFp[20];
   private int dipFpNdx;

   private ByteBuffer buffer;
   byte[] bytes;
   int offset;
   int end;
   int point;

   public FastDtoaBuffer() {
      for (int i = 0; i < diyFps.length; i++) {
         diyFps[i] = new DiyFp(this);
      }
   }

   public FastDtoaBuffer setBuffer(final ByteBuffer buffer) {
      this.buffer = buffer;
      this.bytes = buffer.array();
      this.offset = buffer.position();
      this.end = 0;
      this.dipFpNdx = 0;

      return this;
   }

   DiyFp create() {
      final DiyFp diyFp = diyFps[dipFpNdx++];
      diyFp.reset();
      return diyFp;
   }

   DiyFp create(final long f, final int e) {
      final DiyFp diyFp = diyFps[dipFpNdx++];
      diyFp.reset(f, e);
      return diyFp;
   }

   void append(final byte c) {
      bytes[offset + end++] = c;
   }

   void decreaseLast() {
      bytes[offset + (end - 1)]--;
   }

   public void format(final double value) {
      if (value == 0d) {
         buffer.put((byte) '0');
      }
      else if (FastDtoa.numberToBuffer(value, this)) {
         // check for minus sign
         int firstDigit = bytes[0] == '-' ? 1 : 0;
         int decPoint = point - firstDigit;
         if (decPoint < -5 || decPoint > 21) {
            toExponentialFormat(firstDigit, decPoint);
         } else {
            toFixedFormat(firstDigit, decPoint);
         }
   
         buffer.position(offset + end);
         }
      else {
         // grisu3 waved off formatting the double, so fallback to String.valueOf()
         buffer.put(String.valueOf(value).getBytes());
      }

      buffer = null;
      bytes = null;
   }

   @Override
   public String toString() {
      return "[chars:" + new String(bytes, offset, end) + ", point:" + point + "]";
   }

   private void toFixedFormat(final int firstDigit, final int decPoint) {
      if (point < end) {
         // insert decimal point
         if (decPoint > 0) {
            // >= 1, split decimals and insert point
            System.arraycopy(bytes, offset + point, bytes, offset + point + 1, end - point);
            bytes[offset + point] = '.';
            end++;
         } else {
            // < 1,
            int target = firstDigit + 2 - decPoint;
            System.arraycopy(bytes, offset + firstDigit, bytes, offset + target, end - firstDigit);
            bytes[offset + firstDigit] = '0';
            bytes[offset + firstDigit + 1] = '.';
            if (decPoint < 0) {
               Arrays.fill(bytes, offset + firstDigit + 2, offset + target, (byte) '0');
            }
            end += 2 - decPoint;
         }
      } else if (point > end) {
         // large integer, add trailing zeroes
         Arrays.fill(bytes, offset + end, offset + point, (byte) '0');
         end += point - end;
      }
   }

   private void toExponentialFormat(final int firstDigit, final int decPoint) {
      if (end - firstDigit > 1) {
         // insert decimal point if more than one digit was produced
         int dot = firstDigit + 1;
         System.arraycopy(bytes, offset + dot, bytes, offset + dot + 1, end - dot);
         bytes[offset + dot] = '.';
         end++;
      }
      bytes[offset + end++] = 'e';
      byte sign = '+';
      int exp = decPoint - 1;
      if (exp < 0) {
         sign = '-';
         exp = -exp;
      }
      bytes[offset + end++] = sign;

      int charPos = exp > 99 ? end + 2 : exp > 9 ? end + 1 : end;
      end = charPos + 1;

      // code below is needed because Integer.getChars() is not public
      for (;;) {
         int r = exp % 10;
         bytes[offset + charPos--] = digits[r];
         exp = exp / 10;
         if (exp == 0)
            break;
      }
   }

   final static byte[] digits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };
}
