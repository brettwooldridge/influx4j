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

import com.zaxxer.influx4j.InfluxDB.Precision;
import com.zaxxer.influx4j.util.PrimitiveArraySort;

import static com.zaxxer.influx4j.util.FastValue2Buffer.writeDoubleToBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;

/**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("ALL")
public class Point {
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
   private long timestamp;
   private TimeUnit timeUnit;
   private boolean firstFieldWritten;

   private final PointFactory parentFactory;

   private int tagIndex;

   private int longFieldIndex;
   private int doubleFieldIndex;
   private int stringFieldIndex;
   private int booleanFieldIndex;

   Point(final PointFactory parentFactory) {
      this.parentFactory = parentFactory;
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
      return this;
   }

   public Point field(final String field, final long value) {
      longFields[longFieldIndex++].setPair(field, value);
      return this;
   }

   public Point field(final String field, final double value) {
      doubleFields[doubleFieldIndex++].setPair(field, value);
      return this;
   }

   public Point field(final String field, final boolean value) {
      boolFields[booleanFieldIndex++].setPair(field, value);
      return this;
   }

   public Point timestamp() {
      this.timestamp = System.currentTimeMillis();
      this.timeUnit = TimeUnit.MILLISECONDS;
      return this;
   }

   public Point timestamp(final long timestamp, final TimeUnit timeUnit) {
      this.timestamp = timestamp;
      this.timeUnit = timeUnit;
      return this;
   }

   public Point measurement(final String measurement) {
      this.measurement = measurement;
      return this;
   }

   void write(final ByteBuffer buffer, final Precision precision) {
      final int fieldCount = longFieldIndex + booleanFieldIndex + stringFieldIndex + doubleFieldIndex;

      if (fieldCount == 0) {
         throw new IllegalStateException("Point must have at least one field");
      }
      if (timestamp == 0) {
         throw new IllegalStateException("Point requires a timestamp, default timestamps are not supported");
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
            serializeTag(buffer, tags[ndx]);
         }
      }

      for (int i = 0; i < stringFieldIndex; i++) {
         serializeStringField(buffer, stringFields[i]);
      }

      for (int i = 0; i < longFieldIndex; i++) {
         serializeLongField(buffer, longFields[i]);
      }

      for (int i = 0; i < doubleFieldIndex; i++) {
         serializeDoubleField(buffer, doubleFields[i]);
      }

      for (int i = 0; i < booleanFieldIndex; i++) {
         serializeBooleanField(buffer, boolFields[i]);
      }

      serializeTimestamp(buffer, precision.convert(timestamp, timeUnit));

      buffer.put((byte) '\n');
   }

   void release() {
      // Reset important point state (and bits necessary to aid garbage collection)
      final int tagCount = tagIndex;
      for (int i = 0; i < tagCount; i++) {
         tags[i].reset();
      }

      final int strFieldCount = stringFieldIndex;
      for (int i = 0; i < strFieldCount; i++) {
         stringFields[i].reset();
      }

      tagIndex = 0;
      longFieldIndex = 0;
      stringFieldIndex = 0;
      doubleFieldIndex = 0;
      booleanFieldIndex = 0;
      timestamp = 0;
      firstFieldWritten = false;

      parentFactory.returnPoint(this);
   }


   /*********************************************************************************************
    * Serialization
    */

   private void serializeMeasurement(final ByteBuffer buffer, final String measurement) {
      escapeCommaSpace(buffer, measurement);
   }

   private void serializeTag(final ByteBuffer buffer, final StringPair pair) {
      buffer.put((byte) ',');
      escapeTagKeyOrValue(buffer, pair.name());
      buffer.put((byte) '=');
      escapeTagKeyOrValue(buffer, pair.value());
   }

   private void serializeStringField(final ByteBuffer buffer, final StringPair pair) {
      addFieldSeparator(buffer);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      buffer.put((byte) '"');
      escapeFieldValue(buffer, pair.value());
      buffer.put((byte) '"');
   }

   private void serializeLongField(final ByteBuffer buffer, final LongPair pair) {
      addFieldSeparator(buffer);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      writeLongToBuffer(pair.value(), buffer);
      buffer.put((byte) 'i');
   }

   private void serializeDoubleField(final ByteBuffer buffer, final DoublePair pair) {
      addFieldSeparator(buffer);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      writeDoubleToBuffer(pair.value(), buffer);
   }

   private void serializeBooleanField(final ByteBuffer buffer, final BooleanPair pair) {
      addFieldSeparator(buffer);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      buffer.put(pair.value() ? (byte) 't' : (byte) 'f');
   }

   private void serializeTimestamp(final ByteBuffer buffer, final long timestamp) {
      buffer.put((byte) ' ');
      writeLongToBuffer(timestamp, buffer);
   }

   /*********************************************************************************************
    * Escape handling
    */

   private static void escapeTagKeyOrValue(final ByteBuffer buffer, final String string) {
      escapeCommaEqualSpace(string, buffer);
   }

   private static void escapeFieldKey(final ByteBuffer buffer, final String key) {
      escapeCommaEqualSpace(key, buffer);
   }

   private static void escapeFieldValue(final ByteBuffer buffer, final String value) {
      escapeDoubleQuote(value, buffer);
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

   private static void escapeCommaEqualSpace(final String string, final ByteBuffer buffer) {
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

   private static void escapeDoubleQuote(final String string, final ByteBuffer buffer) {
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

   private static boolean containsCommaSpace(final String string) {
      for (int i = 0; i < string.length(); i++) {
         final char c = string.charAt(i);
         if (c == ' ' || c == ',' || c > '~') return true;
      }
      return false;
   }

   private static boolean containsCommaEqualSpace(final String string) {
      for (int i = 0; i < string.length(); i++) {
         final char c = string.charAt(i);
         if (c == ' ' || c == ',' || c == '=' || c > '~') return true;
      }
      return false;
   }

   /*********************************************************************************************
    * Miscellaneous
    */

   private void addFieldSeparator(final ByteBuffer buffer) {
      if (firstFieldWritten) {
         buffer.put((byte) ',');
      }
      else {
         firstFieldWritten = true;
         buffer.put((byte) ' ');
      }
   }

   private static final class ParallelTagArrayComparator implements PrimitiveArraySort.IntComparator {
      private final StringPair[] tags;

      private ParallelTagArrayComparator(final StringPair[] tags) {
         this.tags = tags;
      }

      @Override
      public int compare(final int a, final int b) {
         return tags[a].name().compareTo(tags[b].name());
      }
   }

   /*********************************************************************************************
    * Pair classes
    */

   private static final class StringPair {
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

   private static final class LongPair {
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
   }

   private static final class DoublePair {
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
   }

   private static final class BooleanPair {
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
   }
}
