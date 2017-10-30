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

import stormpot.Poolable;

import java.nio.ByteBuffer;
import java.util.TreeMap;

import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;

/**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("ALL")
public class Point implements Poolable {
   private final TreeMap<String, String> tags;
   private final AggregateBuffer buffer;
   private Long timestamp;

   public static Point createPoint(final String measurement) {
      // TODO: Point pooling goes here...
      final Point point = new Point();
      point.measurement(measurement);
      return point;
   }

   private Point() {
      this.tags = new TreeMap<>();
      this.buffer = new AggregateBuffer();
   }

   public Point tag(final String tag, final String value) {
      tags.put(tag, value);
      return this;
   }

   public Point field(final String field, final String value) {
      buffer.serializeStringField(field, value);
      return this;
   }

   public Point field(final String field, final long value) {
      buffer.serializeLongField(field, value);
      return this;
   }

   public Point field(final String field, final float value) {
//      fields.put(field, value);
      return this;
   }

   public Point field(final String field, final double value) {
//      fields.put(field, value);
      return this;
   }

   public Point field(final String field, final boolean value) {
//      fields.put(field, value);
      return this;
   }

   public Point timestamp(long timestamp) {
      this.timestamp = timestamp;
      return this;
   }

   @Override
   public void release() {

   }

   void measurement(final String measurement) {
      buffer.serializeMeasurement(measurement);
   }

   void reset() {
      this.tags.clear();
      this.timestamp = null;
   }

   void getFinalBuffer() {
      if (!tags.isEmpty()) {
         // write tags
      }

      if (timestamp != null) {
         buffer.serializeTimestamp(timestamp);
      }
   }

   private static final class AggregateBuffer {
      private static final byte[] TRUE_BYTES = "=true".getBytes();
      private static final byte[] FALSE_BYTES = "=false".getBytes();

      private ByteBuffer tagsBuffer;
      private ByteBuffer dataBuffer;
      private boolean hasField;

      AggregateBuffer() {
         this.tagsBuffer = ByteBuffer.allocate(128);
         this.dataBuffer = ByteBuffer.allocate(128);
      }

      private void reset() {
         tagsBuffer.clear();
         dataBuffer.clear();
         dataBuffer.put((byte) ' ');
         this.hasField = false;
      }

      private void serializeMeasurement(final String measurement) {
         ensureTagBufferCapacity(measurement.length());

         final byte[] bytes = measurement.getBytes();
         for (int i = 0; i < bytes.length; i++) {
            final byte b = bytes[i];
            if (b == ',' || b == ' ') {
               tagsBuffer.put((byte) '\\');
            }
            tagsBuffer.put(b);
         }
      }

      private void serializeStringField(final String field, final String value) {
         addFieldSeparator(field);
         escapeFieldKey(field);
         dataBuffer.put((byte) '=');
         escapeFieldValue(value);
      }

      private void serializeLongField(final String field, final long value) {
         addFieldSeparator(field);
         escapeFieldKey(field);
         dataBuffer.put((byte) '=');
         writeLongToBuffer(value, dataBuffer);
      }

      private void serializeBooleanField(final String field, final boolean value) {
         addFieldSeparator(field);
         escapeFieldKey(field);
         dataBuffer.put(value ? TRUE_BYTES : FALSE_BYTES);
      }

      private void serializeTimestamp(final long timestamp) {
         ensureFieldBufferCapacity(21);
         dataBuffer.put((byte) ' ');
         writeLongToBuffer(timestamp, dataBuffer);
      }

      private void addFieldSeparator(final String field) {
         ensureFieldBufferCapacity(field.length() + 1);
         if (hasField) {
            dataBuffer.put((byte) ',');
         }

         hasField = true;
      }

      /*********************************************************************************************
       * Escape handling
       */

      private void escapeTag(final String str) {
         ensureTagBufferCapacity(str.length());
         escapeCommaEqualSpace(str.getBytes(), tagsBuffer);
      }

      private void escapeFieldKey(final String key) {
         escapeCommaEqualSpace(key.getBytes(), dataBuffer);
      }

      private void escapeFieldValue(final String value) {
         ensureFieldBufferCapacity(value.length());
         escapeaDoubleQuote(value.getBytes(), dataBuffer);
      }

      private void escapeCommaEqualSpace(final byte[] bytes, final ByteBuffer buffer) {
         for (int i = 0; i < bytes.length; i++) {
            switch (bytes[i]) {
               case ',':
               case '=':
               case ' ':
                  buffer.put((byte) '\\');
                  buffer.put(bytes[i]);
                  break;
               default:
                  buffer.put(bytes[i]);
            }
         }
      }

      private void escapeaDoubleQuote(final byte[] bytes, final ByteBuffer buffer) {
         for (int i = 0; i < bytes.length; i++) {
            switch (bytes[i]) {
               case '"':
                  buffer.put((byte) '\\');
                  buffer.put(bytes[i]);
                  break;
               default:
                  buffer.put(bytes[i]);
            }
         }
      }

      /*********************************************************************************************
       * Capacity handling
       */

      private void ensureTagBufferCapacity(final int len) {
         if (tagsBuffer.remaining() < len * 2) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(tagsBuffer.capacity() * 2);
            newBuffer.put(tagsBuffer.array(), 0, tagsBuffer.limit());
            tagsBuffer = newBuffer;
         }
      }

      private void ensureFieldBufferCapacity(final int len) {
         if (dataBuffer.remaining() < len * 2) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(dataBuffer.capacity() * 2);
            newBuffer.put(dataBuffer.array(), 0, dataBuffer.limit());
            dataBuffer = newBuffer;
         }
      }
   }
}
