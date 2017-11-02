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

import com.zaxxer.influx4j.util.PrimitiveArraySort;
import stormpot.Poolable;
import stormpot.Slot;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static com.zaxxer.influx4j.util.FastValue2Buffer.writeDoubleToBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;
import static com.zaxxer.influx4j.util.Utf8.encodedLength;

/**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("ALL")
public class Point implements Poolable, AutoCloseable {
   private final static int MAX_TAG_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);

   private final ParallelTagArrayComparator tagKeyComparator;
   private final AggregateBuffer buffer;
   private final ArrayList<String> tagKeys;
   private final ArrayList<String> tagValues;
   private final int[] tagSort;
   private final Slot slot;
   private Long timestamp;

   Point(final Slot slot) {
      this.slot = slot;
      this.tagKeys = new ArrayList<>();
      this.tagValues = new ArrayList<>();
      this.tagSort = new int[MAX_TAG_COUNT];
      this.buffer = new AggregateBuffer();
      this.tagKeyComparator = new ParallelTagArrayComparator(tagKeys);
   }

   public Point tag(final String tag, final String value) {
      tagKeys.add(tag);
      tagValues.add(value);
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

   public Point field(final String field, final double value) {
      // TODO: zero allocation serialization
       buffer.serializeDoubleField(field, value);
      return this;
   }

   public Point field(final String field, final boolean value) {
      buffer.serializeBooleanField(field, value);
      return this;
   }

   public Point timestamp(final long timestamp, final TimeUnit timeUnit) {
      this.timestamp = timeUnit.toNanos(timestamp);
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

   void measurement(final String measurement) {
      buffer.serializeMeasurement(measurement);
   }

   Point writeToStream(final OutputStream stream) throws IOException {
      if (!buffer.hasField) {
         throw new IllegalStateException("Point must have at least one field");
      }

      if (!tagKeys.isEmpty()) {
         final int tags = tagKeys.size();
         for (int i = 0; i < tags; i++) {
            tagSort[i] = i;
         }

         PrimitiveArraySort.sort(tagSort, tags, tagKeyComparator);
         for (int i = 0; i < tags; i++) {
            final int ndx = tagSort[i];
            buffer.serializeTag(tagKeys.get(ndx), tagValues.get(ndx));
         }
      }

      if (timestamp != null) {
         buffer.serializeTimestamp(timestamp);
      }

      buffer.write(stream);
      return this;
   }

   private void reset() {
      tagKeys.clear();
      tagValues.clear();
      timestamp = null;
      buffer.reset();
   }


   /***************************************************************************
    * Where all the magic happens...
    */

   private static final class AggregateBuffer {
      private static final int INITIAL_BUFFER_SIZES = 128;
      private static final byte[] TRUE_BYTES = "=t".getBytes();
      private static final byte[] FALSE_BYTES = "=f".getBytes();

      private ByteBuffer tagsBuffer;
      private ByteBuffer dataBuffer;
      private boolean hasField;

      AggregateBuffer() {
         this.tagsBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZES);
         this.dataBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZES).put((byte) ' ');
      }

      private void reset() {
         tagsBuffer.clear();
         dataBuffer.clear();
         dataBuffer.put((byte) ' ');
         this.hasField = false;
      }

      /*********************************************************************************************
       * Serialization
       */

      private void serializeMeasurement(final String measurement) {
         ensureTagBufferCapacity(measurement);
         escapeCommaSpace(measurement, tagsBuffer);
      }

      private void serializeTag(final String key, final String value) {
         ensureTagBufferCapacity(2); // , and =

         tagsBuffer.put((byte) ',');
         escapeTagKeyOrValue(key);
         tagsBuffer.put((byte) '=');
         escapeTagKeyOrValue(value);
      }

      private void serializeStringField(final String field, final String value) {
         ensureFieldBufferCapacity(3); // = and two "

         addFieldSeparator();
         escapeFieldKey(field);
         dataBuffer.put((byte) '=');
         dataBuffer.put((byte) '"');
         escapeFieldValue(value);
         dataBuffer.put((byte) '"');
      }

      private void serializeLongField(final String field, final long value) {
         ensureFieldBufferCapacity(3); // = and i

         addFieldSeparator();
         escapeFieldKey(field);
         dataBuffer.put((byte) '=');
         ensureFieldBufferCapacity(21);
         writeLongToBuffer(value, dataBuffer);
         dataBuffer.put((byte) 'i');
      }

      private void serializeDoubleField(final String field, final double value) {
         ensureFieldBufferCapacity(1); // =

         addFieldSeparator();
         escapeFieldKey(field);
         dataBuffer.put((byte) '=');
         ensureFieldBufferCapacity(25);
         writeDoubleToBuffer(value, dataBuffer);
      }

      private void serializeBooleanField(final String field, final boolean value) {
         ensureFieldBufferCapacity(3); // = and two "

         addFieldSeparator();
         escapeFieldKey(field);
         dataBuffer.put(value ? TRUE_BYTES : FALSE_BYTES);
      }

      private void serializeTimestamp(final long timestamp) {
         ensureFieldBufferCapacity(21);
         dataBuffer.put((byte) ' ');
         writeLongToBuffer(timestamp, dataBuffer);
      }

      /*********************************************************************************************
       * Escape handling
       */

      private void escapeTagKeyOrValue(final String tag) {
         ensureTagBufferCapacity(tag);
         escapeCommaEqualSpace(tag, tagsBuffer);
      }

      private void escapeFieldKey(final String key) {
         ensureFieldBufferCapacity(key);
         escapeCommaEqualSpace(key, dataBuffer);
      }

      private void escapeFieldValue(final String value) {
         ensureFieldBufferCapacity(value);
         escapeDoubleQuote(value, dataBuffer);
      }

      private void escapeCommaSpace(final String string, final ByteBuffer buffer) {
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

      private void escapeCommaEqualSpace(final String string, final ByteBuffer buffer) {
         if (containsCommaEqualSpace(string)) {
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

      private void escapeDoubleQuote(final String string, final ByteBuffer buffer) {
         if (string.indexOf('"') != -1) {
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

      private boolean containsCommaSpace(final String string) {
         for (int i = 0; i < string.length(); i++) {
            switch (string.charAt(i)) {
               case ',':
               case ' ':
                  return true;
            }
         }
         return false;
      }

      private boolean containsCommaEqualSpace(final String string) {
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
       * Capacity handling
       */

      private void ensureTagBufferCapacity(final String string) {
         if (tagsBuffer.remaining() < (string.length() * 4)) { // x4 for max. UTF-8 encoded bytes
            ensureTagBufferCapacity(encodedLength(string));
         }
      }

      private void ensureFieldBufferCapacity(final String string) {
         if (dataBuffer.remaining() < (string.length() * 4)) { // x4 for max. UTF-8 encoded bytes
            ensureFieldBufferCapacity(encodedLength(string));
         }
      }

      private void ensureTagBufferCapacity(final int len) {
         if (tagsBuffer.remaining() < len) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(tagsBuffer.capacity() * 2);
            newBuffer.put(tagsBuffer.array(), 0, tagsBuffer.limit());
            tagsBuffer = newBuffer;
         }
      }

      private void ensureFieldBufferCapacity(final int len) {
         if (dataBuffer.remaining() < len) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(dataBuffer.capacity() * 2);
            newBuffer.put(dataBuffer.array(), 0, dataBuffer.limit());
            dataBuffer = newBuffer;
         }
      }

      /*********************************************************************************************
       * Miscellaneous
       */

      private void addFieldSeparator() {
         ensureFieldBufferCapacity(1);
         if (hasField) {
            dataBuffer.put((byte) ',');
         }

         hasField = true;
      }

      private void write(final OutputStream outputStream) throws IOException {
         tagsBuffer.flip();
         dataBuffer.flip();

         outputStream.write(tagsBuffer.array(), 0, tagsBuffer.limit());
         outputStream.write(dataBuffer.array(), 0, dataBuffer.limit());
      }
   }

   private static class ParallelTagArrayComparator implements PrimitiveArraySort.IntComparator {
      private final ArrayList<String> tagKeys;

      private ParallelTagArrayComparator(final ArrayList<String> tagKeys) {
         this.tagKeys = tagKeys;
      }

      @Override
      public int compare(int a, int b) {
         return tagKeys.get(a).compareTo(tagKeys.get(b));
      }
   }
}
