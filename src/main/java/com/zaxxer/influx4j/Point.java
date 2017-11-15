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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.zaxxer.influx4j.util.PrimitiveArraySort;
import stormpot.Poolable;
import stormpot.Slot;

import static com.zaxxer.influx4j.Point.PointSerializer.serializeTag;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeStringField;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeLongField;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeDoubleField;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeBooleanField;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeMeasurement;
import static com.zaxxer.influx4j.Point.PointSerializer.serializeTimestamp;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeDoubleToBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;
import static com.zaxxer.influx4j.util.Utf8.containsUnicode;

/**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("ALL")
public class Point implements Poolable, AutoCloseable {
   private final static int MAX_TAG_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);
   private final static int MAX_FIELD_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);

   private final ParallelTagArrayComparator tagKeyComparator;

   private final StringPair[] tags;
   private final int[] tagSort;

   private final LongPair[] longFields;
   private final StringPair[] stringFields;
   private final DoublePair[] doubleFields;
   private final BooleanPair[] boolFields;

   private String measurement;

   private final Slot slot;

   private Long timestamp;
   private int tagIndex;
   private int tagMark;

   private int longFieldIndex;
   private int doubleFieldIndex;
   private int stringFieldIndex;
   private int booleanFieldIndex;

   Point(final Slot slot) {
      this.slot = slot;
      this.tags = new StringPair[MAX_TAG_COUNT];
      this.tagSort = new int[MAX_TAG_COUNT];
      this.tagKeyComparator = new ParallelTagArrayComparator(tags);

      this.longFields = new LongPair[MAX_FIELD_COUNT];
      this.boolFields = new BooleanPair[MAX_FIELD_COUNT];
      this.stringFields = new StringPair[MAX_FIELD_COUNT];
      this.doubleFields = new DoublePair[MAX_FIELD_COUNT];

      for (int i = 0; i < MAX_FIELD_COUNT; i++) tags[i] = new StringPair();
      for (int i = 0; i < MAX_FIELD_COUNT; i++) longFields[i] = new LongPair();
      for (int i = 0; i < MAX_FIELD_COUNT; i++) boolFields[i] = new BooleanPair();
      for (int i = 0; i < MAX_FIELD_COUNT; i++) stringFields[i] = new StringPair();
      for (int i = 0; i < MAX_FIELD_COUNT; i++) doubleFields[i] = new DoublePair();
   }

   public Point tag(final String tag, final String value) {
      tags[tagIndex++].setPair(tag, value);
      return this;
   }

   public Point field(final String field, final String value) {
      stringFields[stringFieldIndex++].setPair(field, value);
      // buffer.serializeStringField(field, value);
      return this;
   }

   public Point field(final String field, final long value) {
      longFields[longFieldIndex++].setPair(field, value);
      // buffer.serializeLongField(field, value);
      return this;
   }

   public Point field(final String field, final double value) {
      doubleFields[doubleFieldIndex++].setPair(field, value);
       // buffer.serializeDoubleField(field, value);
      return this;
   }

   public Point field(final String field, final boolean value) {
      boolFields[booleanFieldIndex++].setPair(field, value);
      // buffer.serializeBooleanField(field, value);
      return this;
   }

   public Point timestamp(final long timestamp, final TimeUnit timeUnit) {
      this.timestamp = timeUnit.toNanos(timestamp);
      return this;
   }

   public Point mark() {
      tagMark = tagIndex;
      // buffer.mark();
      return this;
   }

   public Point rewind() {
      tagIndex = tagMark;
      // buffer.rewind();
      return this;
   }

   @Override
   public void close() {
      release();
   }

   @Override
   public void release() {
      reset();
      slot.release(this);
   }

   public Point measurement(final String measurement) {
      this.measurement = measurement;
      return this;
   }

   void write(final ByteBuffer buffer) {
      final int fieldCount = longFieldIndex + booleanFieldIndex + stringFieldIndex + doubleFieldIndex;

      if (fieldCount == 0) {
         throw new IllegalStateException("Point must have at least one field");
      }

      serializeMeasurement(buffer, measurement);

      if (tagIndex > 0) {
         final int tagCount = tagIndex;
         for (int i = 0; i < tagCount; i++) {
            tagSort[i] = i;
         }

         PrimitiveArraySort.sort(tagSort, tagCount, tagKeyComparator);
         for (int i = 0; i < tagCount; i++) {
            final int ndx = tagSort[i];
            final StringPair pair = tags[ndx];
            serializeTag(buffer, pair.name(), pair.value());
         }
      }

      boolean notFirstField = false;
      for (int i = 0; i < stringFieldIndex; i++) {
         final StringPair pair = stringFields[i];
         serializeStringField(buffer, pair.name(), pair.value(), notFirstField);
         notFirstField = true;
      }

      for (int i = 0; i < longFieldIndex; i++) {
         final LongPair pair = longFields[i];
         serializeLongField(buffer, pair.name(), pair.value(), notFirstField);
         notFirstField = true;
      }

      for (int i = 0; i < doubleFieldIndex; i++) {
         final DoublePair pair = doubleFields[i];
         serializeDoubleField(buffer, pair.name(), pair.value(), notFirstField);
         notFirstField = true;
      }

      for (int i = 0; i < booleanFieldIndex; i++) {
         final BooleanPair pair = boolFields[i];
         serializeBooleanField(buffer, pair.name(), pair.value(), notFirstField);
         notFirstField = true;
      }

      if (timestamp != null) {
         serializeTimestamp(buffer, timestamp);
      }

      buffer.put((byte) '\n');
   }

   void reset() {
      final int len = tagIndex;
      for (int i = 0; i < len; i++) {
         tags[i].reset();
      }

      tagMark = 0;
      tagIndex = 0;
      longFieldIndex = 0;
      stringFieldIndex = 0;
      doubleFieldIndex = 0;
      booleanFieldIndex = 0;

      timestamp = null;
   }


   /***************************************************************************
    * Where all the magic happens...
    */

   static class PointSerializer {
      // private static final int MAX_BUFFER_COUNTS = Integer.getInteger("com.zaxxer.influx4j.maxBuffersPerPoint", 64);

      // private final BufferPoolManager bufferPool;
      // private final PoolableByteBuffer[] poolTagBuffers;
      // private final PoolableByteBuffer[] poolFieldBuffers;
      // private ByteBuffer tagsBuffer;
      // private ByteBuffer fieldBuffer;
      // private boolean hasField;
      // private int tagBufferNdx;
      // private int fieldBufferNdx;

      // AggregateBuffer(final BufferPoolManager bufferPool) {
      //    this.bufferPool = bufferPool;
      //    this.poolTagBuffers = new PoolableByteBuffer[MAX_BUFFER_COUNTS];
      //    this.poolFieldBuffers = new PoolableByteBuffer[MAX_BUFFER_COUNTS];
      // }

      // private void mark() {
      //    tagsBuffer.mark();
      //    fieldBuffer.mark();
      // }

      // private void rewind() {
      //    tagsBuffer.reset();
      //    fieldBuffer.reset();
      // }

      // private void reset() {
      //    Arrays.fill(poolTagBuffers, null);
      //    Arrays.fill(poolFieldBuffers, null);

      //    tagBufferNdx = 0;
      //    fieldBufferNdx = 0;

      //    PoolableByteBuffer tmpTagBuffer = bufferPool.borrow128Buffer();
      //    poolTagBuffers[tagBufferNdx++] = tmpTagBuffer;
      //    tagsBuffer = tmpTagBuffer.getBuffer();

      //    tmpTagBuffer = bufferPool.borrow128Buffer();
      //    poolFieldBuffers[fieldBufferNdx++] = tmpTagBuffer;
      //    fieldBuffer = tmpTagBuffer.getBuffer();
      //    fieldBuffer.put((byte) ' ');
      //    hasField = false;
      // }

      // private int enqueueBuffers(final ArrayList<PoolableByteBuffer> buffers) {
      //    ensureFieldBufferCapacity(1);
      //    fieldBuffer.put((byte) '\n');

      //    int contentLength = 0;
      //    for (int i = 0; i < tagBufferNdx; i++) {
      //       final PoolableByteBuffer buffer = poolTagBuffers[i];
      //       contentLength += buffer.getBuffer().flip().remaining();
      //       buffers.add(buffer);
      //    }
      //    for (int i = 0; i < fieldBufferNdx; i++) {
      //       final PoolableByteBuffer buffer = poolFieldBuffers[i];
      //       contentLength += buffer.getBuffer().flip().remaining();
      //       buffers.add(buffer);
      //    }
      //    return contentLength;
      // }

      /*********************************************************************************************
       * Serialization
       */

      static void serializeMeasurement(final ByteBuffer buffer, final String measurement) {
         escapeCommaSpace(buffer, measurement);
      }

      static void serializeTag(final ByteBuffer buffer, final String key, final String value) {
         buffer.put((byte) ',');
         escapeTagKeyOrValue(buffer, key, containsUnicode(key));
         buffer.put((byte) '=');
         escapeTagKeyOrValue(buffer, value, containsUnicode(value));
      }

      static void serializeStringField(final ByteBuffer buffer, final String field, final String value, final boolean notFirstField) {
         addFieldSeparator(buffer, notFirstField);
         escapeFieldKey(buffer, field, containsUnicode(field));
         buffer.put((byte) '=');
         buffer.put((byte) '"');
         escapeFieldValue(buffer, value, containsUnicode(value));
         buffer.put((byte) '"');
      }

      static void serializeLongField(final ByteBuffer buffer, final String field, final long value, final boolean notFirstField) {
         addFieldSeparator(buffer, notFirstField);
         escapeFieldKey(buffer, field, containsUnicode(field));
         buffer.put((byte) '=');
         writeLongToBuffer(value, buffer);
         buffer.put((byte) 'i');
      }

      static void serializeDoubleField(final ByteBuffer buffer, final String field, final double value, final boolean notFirstField) {
         addFieldSeparator(buffer, notFirstField);
         escapeFieldKey(buffer, field, containsUnicode(field));
         buffer.put((byte) '=');
         writeDoubleToBuffer(value, buffer);
      }

      static void serializeBooleanField(final ByteBuffer buffer, final String field, final boolean value, final boolean notFirstField) {
         addFieldSeparator(buffer, notFirstField);
         escapeFieldKey(buffer, field, containsUnicode(field));
         buffer.put((byte) '=');
         buffer.put(value ? (byte) 't' : (byte) 'f');
      }

      static void serializeTimestamp(final ByteBuffer buffer, final long timestamp) {
         buffer.put((byte) ' ');
         writeLongToBuffer(timestamp, buffer);
      }

      /*********************************************************************************************
       * Escape handling
       */

      private static void escapeTagKeyOrValue(final ByteBuffer buffer, final String string, final boolean isUnicode) {
         escapeCommaEqualSpace(string, buffer, isUnicode);
      }

      private static void escapeFieldKey(final ByteBuffer buffer, final String key, final boolean isUnicode) {
         escapeCommaEqualSpace(key, buffer, isUnicode);
      }

      private static void escapeFieldValue(final ByteBuffer buffer, final String value, final boolean isUnicode) {
         escapeDoubleQuote(value, buffer, isUnicode);
      }

      private static void escapeCommaSpace(final ByteBuffer buffer, final String string) {
         if (containsCommaSpace(string)) {
            final byte[] bytes = string.getBytes();
            for (int i = 0; i < bytes.length; i++) {
               switch (bytes[i]) {
                  case ',':
                  case ' ':
                     buffer.put((byte) '\\');
                     buffer.put(bytes[i]);
                     break;
                  default:
                     buffer.put(bytes[i]);
               }
            }
         }
         else {
            final int pos = buffer.position();
            string.getBytes(0, string.length(), buffer.array(), pos);
            buffer.position(pos + string.length());
         }
      }

      private static void escapeCommaEqualSpace(final String string, final ByteBuffer buffer, final boolean isUnicode) {
         if (isUnicode || containsCommaEqualSpace(string)) {
            final byte[] bytes = string.getBytes();
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
         else {
            final int pos = buffer.position();
            string.getBytes(0, string.length(), buffer.array(), pos);
            buffer.position(pos + string.length());
         }
      }

      private static void escapeDoubleQuote(final String string, final ByteBuffer buffer, final boolean isUnicode) {
         if (isUnicode || string.indexOf('"') != -1) {
            final byte[] bytes = string.getBytes();
            for (int i = 0; i < bytes.length; i++) {
               if (bytes[i] == '"') {
                  buffer.put((byte) '\\');
               }
               buffer.put(bytes[i]);
            }
         }
         else {
            final int pos = buffer.position();
            string.getBytes(0, string.length(), buffer.array(), pos);
            buffer.position(pos + string.length());
         }
      }

      private static boolean containsCommaSpace(final String string) {
         for (int i = 0; i < string.length(); i++) {
            switch (string.charAt(i)) {
               case ',':
               case ' ':
                  return true;
            }
         }
         return false;
      }

      private static boolean containsCommaEqualSpace(final String string) {
         for (int i = 0; i < string.length(); i++) {
            switch (string.charAt(i)) {
               case ',':
               case '=':
               case ' ':
                  return true;
            }
         }
         return false;
      }

      /*********************************************************************************************
       * Miscellaneous
       */

      private static void addFieldSeparator(final ByteBuffer buffer, final boolean notFirstField) {
         buffer.put((byte) (notFirstField ? ',' : ' '));
      }
   }

   private static class ParallelTagArrayComparator implements PrimitiveArraySort.IntComparator {
      private final StringPair[] tags;

      private ParallelTagArrayComparator(final StringPair[] tags) {
         this.tags = tags;
      }

      @Override
      public int compare(int a, int b) {
         return tags[a].name().compareTo(tags[b].name());
      }
   }

   /*********************************************************************************************
    * Pair classes
    */

   private static class StringPair {
      private String name;
      private String value;

      void setPair(final String name, final String value) {
         this.name = name;
         this.value = value;
      }

      String name() {
         return name;
      }

      String value() {
         return value;
      }

      void reset() {
         name = null;
         value = null;
      }
   }

   private static class LongPair {
      private String name;
      private long value;

      void setPair(final String name, final long value) {
         this.name = name;
         this.value = value;
      }

      String name() {
         return name;
      }

      long value() {
         return value;
      }

      void reset() {
         name = null;
      }
   }

   private static class DoublePair {
      private String name;
      private double value;

      void setPair(final String name, final double value) {
         this.name = name;
         this.value = value;
      }

      String name() {
         return name;
      }

      double value() {
         return value;
      }

      void reset() {
         name = null;
      }
   }

   private static class BooleanPair {
      private String name;
      private boolean value;

      void setPair(final String name, final boolean value) {
         this.name = name;
         this.value = value;
      }

      String name() {
         return name;
      }

      boolean value() {
         return value;
      }

      void reset() {
         name = null;
      }
   }
}
