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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.zaxxer.influx4j.util.PrimitiveArraySort;
import org.jctools.queues.MessagePassingQueue;
import stormpot.Poolable;
import stormpot.Slot;

import static com.zaxxer.influx4j.BufferPoolManager.PoolableByteBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeDoubleToBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;
import static com.zaxxer.influx4j.util.Utf8.containsUnicode;
import static com.zaxxer.influx4j.util.Utf8.encodedLength;

/**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("ALL")
public class Point implements Poolable, AutoCloseable {
   private final static int MAX_TAG_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);

   private final ParallelTagArrayComparator tagKeyComparator;
   private final AggregateBuffer buffer;
   private final String[] tagKeys;
   private final String[] tagValues;
   private final int[] tagSort;
   private final Slot slot;

   private Long timestamp;
   private int tagKeyIndex;
   private int tagKeyMark;

   Point(final Slot slot, final BufferPoolManager bufferPool) {
      this.slot = slot;
      this.tagKeys = new String[MAX_TAG_COUNT];
      this.tagSort = new int[MAX_TAG_COUNT];
      this.tagValues = new String[MAX_TAG_COUNT];
      this.tagKeyComparator = new ParallelTagArrayComparator(tagKeys);
      this.buffer = new AggregateBuffer(bufferPool);
      this.buffer.reset();
   }

   public Point tag(final String tag, final String value) {
      tagKeys[tagKeyIndex] = tag;
      tagValues[tagKeyIndex] = value;
      tagKeyIndex++;
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

   public Point mark() {
      tagKeyMark = tagKeyIndex;
      buffer.mark();
      return this;
   }

   public Point rewind() {
      tagKeyIndex = tagKeyMark;
      buffer.rewind();
      return this;
   }

   public Point write(InfluxDB influxDB) {
      influxDB.write(this);
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
      buffer.serializeMeasurement(measurement);
      return this;
   }

   int enqueueBuffers(final ArrayList<PoolableByteBuffer> buffers) {
      finalizeBuffers();

      return buffer.enqueueBuffers(buffers);
   }

   void finalizeBuffers() {
      if (!buffer.hasField) {
         throw new IllegalStateException("Point must have at least one field");
      }

      if (tagKeyIndex > 0) {
         final int tagCount = tagKeyIndex;
         for (int i = 0; i < tagCount; i++) {
            tagSort[i] = i;
         }

         PrimitiveArraySort.sort(tagSort, tagCount, tagKeyComparator);
         for (int i = 0; i < tagCount; i++) {
            final int ndx = tagSort[i];
            buffer.serializeTag(tagKeys[ndx], tagValues[ndx]);
         }
      }

      if (timestamp != null) {
         buffer.serializeTimestamp(timestamp);
      }
   }

   void reset() {
      Arrays.fill(tagKeys, null);
      Arrays.fill(tagValues, null);
      tagKeyIndex = 0;
      tagKeyMark = 0;
      timestamp = null;
      buffer.reset();
   }

   /**
    * Used for testing.
    */
   final ArrayList<PoolableByteBuffer> testing = new ArrayList<>();

   Point writeToStream(final OutputStream os) throws IOException {
      enqueueBuffers(testing);

      final int len = testing.size();
      for (int i = 0; i < len; i++) {
         final PoolableByteBuffer poolableBuffer = testing.get(i);
         final ByteBuffer buffer = poolableBuffer.getBuffer();
         os.write(buffer.array(), 0, buffer.limit());
         poolableBuffer.release();
      }

      testing.clear();
      return this;
   }

   /***************************************************************************
    * Where all the magic happens...
    */

   private static final class AggregateBuffer {
      private static final int MAX_BUFFER_COUNTS = Integer.getInteger("com.zaxxer.influx4j.maxBuffersPerPoint", 64);

      private final BufferPoolManager bufferPool;
      private final PoolableByteBuffer[] poolTagBuffers;
      private final PoolableByteBuffer[] poolFieldBuffers;
      private ByteBuffer tagsBuffer;
      private ByteBuffer fieldBuffer;
      private boolean hasField;
      private int tagBufferNdx;
      private int fieldBufferNdx;

      AggregateBuffer(final BufferPoolManager bufferPool) {
         this.bufferPool = bufferPool;
         this.poolTagBuffers = new PoolableByteBuffer[MAX_BUFFER_COUNTS];
         this.poolFieldBuffers = new PoolableByteBuffer[MAX_BUFFER_COUNTS];
      }

      private void mark() {
         tagsBuffer.mark();
         fieldBuffer.mark();
      }

      private void rewind() {
         tagsBuffer.reset();
         fieldBuffer.reset();
      }

      private void reset() {
         Arrays.fill(poolTagBuffers, null);
         Arrays.fill(poolFieldBuffers, null);

         tagBufferNdx = 0;
         fieldBufferNdx = 0;

         PoolableByteBuffer tmpTagBuffer = bufferPool.borrow128Buffer();
         poolTagBuffers[tagBufferNdx++] = tmpTagBuffer;
         tagsBuffer = tmpTagBuffer.getBuffer();

         tmpTagBuffer = bufferPool.borrow128Buffer();
         poolFieldBuffers[fieldBufferNdx++] = tmpTagBuffer;
         fieldBuffer = tmpTagBuffer.getBuffer();
         fieldBuffer.put((byte) ' ');
         hasField = false;
      }

      private int enqueueBuffers(final ArrayList<PoolableByteBuffer> buffers) {
         ensureFieldBufferCapacity(1);
         fieldBuffer.put((byte) '\n');

         int contentLength = 0;
         for (int i = 0; i < tagBufferNdx; i++) {
            final PoolableByteBuffer buffer = poolTagBuffers[i];
            contentLength += buffer.getBuffer().flip().remaining();
            buffers.add(buffer);
         }
         for (int i = 0; i < fieldBufferNdx; i++) {
            final PoolableByteBuffer buffer = poolFieldBuffers[i];
            contentLength += buffer.getBuffer().flip().remaining();
            buffers.add(buffer);
         }
         return contentLength;
      }

      /*********************************************************************************************
       * Serialization
       */

      private void serializeMeasurement(final String measurement) {
         tagsBuffer.position(0);

         final boolean isUnicode = containsUnicode(measurement);
         ensureTagBufferCapacity(measurement, isUnicode);
         escapeCommaSpace(measurement, tagsBuffer);
      }

      private void serializeTag(final String key, final String value) {
         ensureTagBufferCapacity(2); // , and =

         tagsBuffer.put((byte) ',');
         escapeTagKeyOrValue(key, containsUnicode(key));
         tagsBuffer.put((byte) '=');
         escapeTagKeyOrValue(value, containsUnicode(value));
      }

      private void serializeStringField(final String field, final String value) {
         ensureFieldBufferCapacity(3); // = and two "

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         fieldBuffer.put((byte) '=');
         fieldBuffer.put((byte) '"');
         escapeFieldValue(value, containsUnicode(value));
         fieldBuffer.put((byte) '"');
      }

      private void serializeLongField(final String field, final long value) {
         ensureFieldBufferCapacity(3); // = and i

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         fieldBuffer.put((byte) '=');
         ensureFieldBufferCapacity(21);
         writeLongToBuffer(value, fieldBuffer);
         fieldBuffer.put((byte) 'i');
      }

      private void serializeDoubleField(final String field, final double value) {
         ensureFieldBufferCapacity(1); // =

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         fieldBuffer.put((byte) '=');
         ensureFieldBufferCapacity(25);
         writeDoubleToBuffer(value, fieldBuffer);
      }

      private void serializeBooleanField(final String field, final boolean value) {
         ensureFieldBufferCapacity(3); // = and two "

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         fieldBuffer.put((byte) '=');
         fieldBuffer.put(value ? (byte) 't' : (byte) 'f');
      }

      private void serializeTimestamp(final long timestamp) {
         ensureFieldBufferCapacity(21);
         fieldBuffer.put((byte) ' ');
         writeLongToBuffer(timestamp, fieldBuffer);
      }

      /*********************************************************************************************
       * Escape handling
       */

      private void escapeTagKeyOrValue(final String string, final boolean isUnicode) {
         ensureTagBufferCapacity(string, isUnicode);
         escapeCommaEqualSpace(string, tagsBuffer, isUnicode);
      }

      private void escapeFieldKey(final String key, final boolean isUnicode) {
         ensureFieldBufferCapacity(key, isUnicode);
         escapeCommaEqualSpace(key, fieldBuffer, isUnicode);
      }

      private void escapeFieldValue(final String value, final boolean isUnicode) {
         ensureFieldBufferCapacity(value, isUnicode);
         escapeDoubleQuote(value, fieldBuffer, isUnicode);
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

      private void escapeCommaEqualSpace(final String string, final ByteBuffer buffer, final boolean isUnicode) {
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

      private void escapeDoubleQuote(final String string, final ByteBuffer buffer, final boolean isUnicode) {
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

      private void ensureTagBufferCapacity(final String string, final boolean isUnicode) {
         if (isUnicode) {
            ensureTagBufferCapacity(encodedLength(string));
         }
         else {
            ensureTagBufferCapacity(string.length());
         }
      }

      private void ensureFieldBufferCapacity(final String string, final boolean isUnicode) {
         if (isUnicode) {
            ensureFieldBufferCapacity(encodedLength(string));
         }
         else {
            ensureFieldBufferCapacity(string.length());
         }
      }

      private void ensureTagBufferCapacity(final int len) {
         if (tagsBuffer.remaining() < len) {
            final PoolableByteBuffer newBuffer;
            if (tagBufferNdx > 7) {
               newBuffer = bufferPool.borrow4096Buffer();
            }
            else if (tagBufferNdx > 3) {
               newBuffer = bufferPool.borrow512Buffer();
            }
            else {
               newBuffer = bufferPool.borrow128Buffer();
            }

            poolTagBuffers[tagBufferNdx++] = newBuffer;
            tagsBuffer = newBuffer.getBuffer();
         }
      }

      private void ensureFieldBufferCapacity(final int len) {
         if (fieldBuffer.remaining() < len) {
            final PoolableByteBuffer newBuffer;
            if (fieldBufferNdx > 7) {
               newBuffer = bufferPool.borrow4096Buffer();
            }
            else if (fieldBufferNdx > 3) {
               newBuffer = bufferPool.borrow512Buffer();
            }
            else {
               newBuffer = bufferPool.borrow128Buffer();
            }

            poolFieldBuffers[fieldBufferNdx++] = newBuffer;
            fieldBuffer = newBuffer.getBuffer();
         }
      }

      /*********************************************************************************************
       * Miscellaneous
       */

      private void addFieldSeparator() {
         ensureFieldBufferCapacity(1);
         if (hasField) {
            fieldBuffer.put((byte) ',');
         }

         hasField = true;
      }

      private void enqueueForWrite(final MessagePassingQueue<PoolableByteBuffer> queue) {
         for (int i = 0; i < tagBufferNdx; i++) {
            if (!queue.offer(poolTagBuffers[i])) {
               throw new RuntimeException("Queue capacity exceeded");
            }
         }

         for (int i = 0; i < fieldBufferNdx; i++) {
            if (!queue.offer(poolFieldBuffers[i])) {
               throw new RuntimeException("Queue capacity exceeded");
            }
         }
      }
   }

   private static class ParallelTagArrayComparator implements PrimitiveArraySort.IntComparator {
      private final String[] tagKeys;

      private ParallelTagArrayComparator(final String[] tagKeys) {
         this.tagKeys = tagKeys;
      }

      @Override
      public int compare(int a, int b) {
         return tagKeys[a].compareTo(tagKeys[b]);
      }
   }
}
