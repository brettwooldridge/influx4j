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
import java.nio.channels.GatheringByteChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

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

   Point(final Slot slot) {
      this.slot = slot;
      this.tagKeys = new String[MAX_TAG_COUNT];
      this.tagSort = new int[MAX_TAG_COUNT];
      this.tagValues = new String[MAX_TAG_COUNT];
      this.buffer = new AggregateBuffer();
      this.tagKeyComparator = new ParallelTagArrayComparator(tagKeys);
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

   void finalizePoint(final ByteBuffer byteBuffer) {

   }

   int writeToChannel(final GatheringByteChannel channel) throws IOException {
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

      return buffer.write(channel);
   }

   Point writeToStream(final OutputStream stream) throws IOException {
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

      buffer.write(stream);
      return this;
   }

   private void reset() {
      Arrays.fill(tagKeys, null);
      Arrays.fill(tagValues, null);
      tagKeyIndex = 0;
      tagKeyMark = 0;
      timestamp = null;
      buffer.reset();
   }


   /***************************************************************************
    * Where all the magic happens...
    */

   private static final class AggregateBuffer {
      private static final int INITIAL_BUFFER_SIZES = 256;

      private ByteBuffer tagsBuffer;
      private ByteBuffer dataBuffer;
      private ByteBuffer[] unitedBuffers;
      private boolean hasField;

      AggregateBuffer() {
         this.tagsBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZES);
         this.dataBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZES).put((byte) ' ');
         unitedBuffers = new ByteBuffer[] {tagsBuffer, dataBuffer};
      }

      private void mark() {
         tagsBuffer.mark();
         dataBuffer.mark();
      }

      private void rewind() {
         tagsBuffer.reset();
         dataBuffer.reset();
      }

      private void reset() {
         tagsBuffer.clear();
         dataBuffer.clear();
         dataBuffer.put((byte) ' ');
         hasField = false;
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
         dataBuffer.put((byte) '=');
         dataBuffer.put((byte) '"');
         escapeFieldValue(value, containsUnicode(value));
         dataBuffer.put((byte) '"');
      }

      private void serializeLongField(final String field, final long value) {
         ensureFieldBufferCapacity(3); // = and i

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         dataBuffer.put((byte) '=');
         ensureFieldBufferCapacity(21);
         writeLongToBuffer(value, dataBuffer);
         dataBuffer.put((byte) 'i');
      }

      private void serializeDoubleField(final String field, final double value) {
         ensureFieldBufferCapacity(1); // =

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         dataBuffer.put((byte) '=');
         ensureFieldBufferCapacity(25);
         writeDoubleToBuffer(value, dataBuffer);
      }

      private void serializeBooleanField(final String field, final boolean value) {
         ensureFieldBufferCapacity(3); // = and two "

         addFieldSeparator();
         escapeFieldKey(field, containsUnicode(field));
         dataBuffer.put((byte) '=');
         dataBuffer.put(value ? (byte) 't' : (byte) 'f');
      }

      private void serializeTimestamp(final long timestamp) {
         ensureFieldBufferCapacity(21);
         dataBuffer.put((byte) ' ');
         writeLongToBuffer(timestamp, dataBuffer);
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
         escapeCommaEqualSpace(key, dataBuffer, isUnicode);
      }

      private void escapeFieldValue(final String value, final boolean isUnicode) {
         ensureFieldBufferCapacity(value, isUnicode);
         escapeDoubleQuote(value, dataBuffer, isUnicode);
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
            final ByteBuffer newBuffer = ByteBuffer.allocate(tagsBuffer.capacity() * 4);
            newBuffer.put(tagsBuffer.array(), 0, tagsBuffer.limit());
            tagsBuffer = newBuffer;
         }
      }

      private void ensureFieldBufferCapacity(final int len) {
         if (dataBuffer.remaining() < len) {
            final ByteBuffer newBuffer = ByteBuffer.allocate(dataBuffer.capacity() * 4);
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
         outputStream.write(tagsBuffer.array(), 0, tagsBuffer.position());
         outputStream.write(dataBuffer.array(), 0, dataBuffer.position());
      }

      private int write(final GatheringByteChannel channel) throws IOException {
         final int tagsBufLen = tagsBuffer.position();
         final int dataBufLen = dataBuffer.position();

         tagsBuffer.position(0);
         dataBuffer.position(0);
         tagsBuffer.limit(tagsBufLen);
         dataBuffer.limit(dataBufLen);

         channel.write(unitedBuffers);

         tagsBuffer.limit(tagsBuffer.capacity());
         dataBuffer.limit(dataBuffer.capacity());
         tagsBuffer.position(tagsBufLen);
         dataBuffer.position(dataBufLen);

         return tagsBufLen + dataBufLen;
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
