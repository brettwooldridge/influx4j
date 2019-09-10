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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.influx4j.InfluxDB.Precision;
import com.zaxxer.influx4j.util.PrimitiveArraySort;

import static com.zaxxer.influx4j.InfluxDB.MAXIMUM_SERIALIZED_POINT_SIZE;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeDoubleToBuffer;
import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;

/**
 * An instance of this class represents a single <i>measurement</i> and associated <i>tags</i>,
 * <i>fields</i>, and <i>timestamp</i> to be persisted in InfluxDB via the {@link InfluxDB#write(Point)}
 * method.
 *
 * @author brett.wooldridge at gmail.com
 */
public class Point implements AutoCloseable {
   private final static int MAX_TAG_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);
   private final static int MAX_FIELD_COUNT = Integer.getInteger("com.zaxxer.influx4j.maxTagCount", 64);

   private final ParallelTagArrayComparator tagKeyComparator;
   private final AtomicInteger retentionCount;

   private final StringPair[] tags;
   private final int[] tagSort;

   private final LongPair[] longFields;
   private final StringPair[] stringFields;
   private final DoublePair[] doubleFields;
   private final BooleanPair[] boolFields;

   private String measurement;
   private long timestamp;
   private TimeUnit timeUnit;

   private final PointFactory parentFactory;

   private int tagIndex;

   private int longFieldIndex;
   private int doubleFieldIndex;
   private int stringFieldIndex;
   private int booleanFieldIndex;

   long sequence;

   Point(final PointFactory parentFactory) {
      this.parentFactory = parentFactory;
      this.tags = new StringPair[MAX_TAG_COUNT];
      this.tagSort = new int[MAX_TAG_COUNT];
      this.retentionCount = new AtomicInteger(1);
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

   /**
    * Tag the {@link Point} with the specified string value.
    * @param tag the name of the tag
    * @param value the string value associated with the tag
    * @return this {@link Point}
    */
   public Point tag(final String tag, final String value) {
      if (tag != null && value != null) {
         tags[tagIndex++].setPair(tag, value);
      }
      return this;
   }

   /**
    * Add a string field to the {@link Point} with the specified value.
    * @param field the name of the field
    * @param value the string value associated with the field
    * @return this {@link Point}
    */
   public Point field(final String field, final String value) {
      if (field != null && value != null) {
         stringFields[stringFieldIndex++].setPair(field, value);
      }
      return this;
   }

   /**
    * Add a long integer field to the {@link Point} with the specified value.
    * @param field the name of the field
    * @param value the long value associated with the field
    * @return this {@link Point}
    */
   public Point field(final String field, final long value) {
      if (field != null) {
         longFields[longFieldIndex++].setPair(field, value);
      }
      return this;
   }

   /**
    * Add a floating point double field to the {@link Point} with the specified value.
    * @param field the name of the field
    * @param value the double value associated with the field
    * @return this {@link Point}
    */
   public Point field(final String field, final double value) {
      if (field != null) {
         doubleFields[doubleFieldIndex++].setPair(field, value);
      }
      return this;
   }

   /**
    * Add a boolean field to the {@link Point} with the specified value.
    * @param field the name of the field
    * @param value the boolean value associated with the field
    * @return this {@link Point}
    */
   public Point field(final String field, final boolean value) {
      if (field != null) {
         boolFields[booleanFieldIndex++].setPair(field, value);
      }
      return this;
   }

   /**
    * Timestamp the {@link Point} with the millisecond resolution time value returned
    * by {@link System#currentTimeMillis()}.
    * @return this {@link Point}
    */
   public Point timestamp() {
      this.timestamp = System.currentTimeMillis();
      this.timeUnit = TimeUnit.MILLISECONDS;
      return this;
   }

   /**
    * Timestamp the {@link Point} with the specified time value and {@link TimeUnit} resolution.
    * @param timestamp the time value
    * @param timeUnit the resolution of the time value
    * @return this {@link Point}
    */
   public Point timestamp(final long timestamp, final TimeUnit timeUnit) {
      this.timestamp = timestamp;
      this.timeUnit = timeUnit;
      return this;
   }

   /**
    * Get the timestamp of this {@link Point} in milliseconds.
    * @return this {@link Point} timestamp in milliseconds
    */
   public long getTimestamp() {
      return timeUnit.toMillis(timestamp);
   }

   /**
    * Set the measurement name of this {@link Point}.
    * @param measurement the new measurement name
    * @return this {@link Point}
    */
   public Point measurement(final String measurement) {
      if (measurement != null) {
         this.measurement = measurement;
      }
      return this;
   }

   /**
    * Get the measurement name of this {@link Point}.
    * @return the measurement name
    */
   public String measurement() {
      return measurement;
   }

   /**
    * Remove a tag from this {@link Point}.
    * @param tag the name of the tag to remove
    * @return this {@link Point}
    */
   public Point removeTag(final String tag) {
      for (int i = 0; i < tagIndex; i++) {
         if (tag.equals(tags[i].name)) {
            if (tagIndex > 1) {
               final StringPair removed = tags[i];
               System.arraycopy(tags, i + 1, tags, i, tagIndex - i - 1);
               tags[tagIndex - 1] = removed;
            }
            --tagIndex;
            break;
         }
      }
      return this;
   }

   /**
    * Get the value of the specified long integer field as an auto-boxed {@link Long}.  If no
    * long integer field was set on this {@link Point}, the return value will be {@code null}.
    * @param field the name of the long integer field
    * @return the long integer field value or {@code null}
    */
   public Long longField(final String field) {
      for (int i = 0; i < longFieldIndex; i++) {
         if (field.equals(longFields[i].name)) {
            return longFields[i].value;
         }
      }
      return null;
   }

   /**
    * Get the value of the specified long integer field, by internal index (see {@link #getLongFieldIndex(String)}),
    * as an auto-boxed {@link Long}.  If no such long integer field was set on this {@link Point}, the return value
    * will be {@code null}.
    * @param index the internal index of the long integer field
    * @return the long integer field value or {@code null}
    */
    public Long longField(final int index) {
      return (index < longFieldIndex) ? longFields[index].value : null;
   }

   /**
    * Get the name of the specified long integer field, by internal index.  If no such long integer field was set
    * on this {@link Point}, the return value will be {@code null}.
    * @param index the internal index of the long integer field
    * @return the name of the long integer field or {@code null}
    */
   public String longFieldName(final int index) {
      return (longFieldIndex > 0) ? longFields[index].name() : null;
   }

   /**
    * Get the value of the specified floating point double field as an auto-boxed {@link Double}.
    * If no double field was set on this {@link Point}, the return value will be {@code null}.
    * @param field the name of the double field
    * @return the double field value or {@code null}
    */
   public Double doubleField(final String field) {
      for (int i = 0; i < doubleFieldIndex; i++) {
         if (field.equals(doubleFields[i].name)) {
            return doubleFields[i].value;
         }
      }
      return null;
   }

   /**
    * Get the value of the specified double field, by internal index (see {@link #getDoubleFieldIndex(String)}),
    * as an auto-boxed {@link Double}.  If no such double field was set on this {@link Point}, the return value
    * will be {@code null}.
    * @param index the internal index of the double field
    * @return the double field value or {@code null}
    */
    public Double doubleField(final int index) {
      return (index < doubleFieldIndex) ? doubleFields[index].value : null;
   }

   /**
    * Get the name of the specified double field, by internal index.  If no such double field was set
    * on this {@link Point}, the return value will be {@code null}.
    * @param index the internal index of the double field
    * @return the name of the double field or {@code null}
    */
    public String doubleFieldName(final int index) {
      return (doubleFieldIndex > 0) ? doubleFields[index].name() : null;
   }

   /**
    * Get the value of the specified boolean field as an auto-boxed {@link Boolean}.  If no
    * boolean field was set on this {@link Point}, the return value will be {@code null}.
    * @param field the name of the boolean field
    * @return the boolean field value or {@code null}
    */
    public Boolean booleanField(final String field) {
      for (int i = 0; i < booleanFieldIndex; i++) {
         if (field.equals(boolFields[i].name)) {
            return boolFields[i].value;
         }
      }
      return null;
   }

   /**
    * Get the value of the specified boolean field, by internal index (see {@link #getBooleanFieldIndex(String)}),
    * as an auto-boxed {@link Boolean}.  If no such boolean field was set on this {@link Point}, the return value
    * will be {@code null}.
    * @param index the internal index of the boolean field
    * @return the boolean field value or {@code null}
    */
    public Boolean booleanField(final int index) {
      return (index < booleanFieldIndex) ? boolFields[index].value : null;
   }

   /**
    * Get the name of the specified boolean field, by internal index.  If no such boolean field was set
    * on this {@link Point}, the return value will be {@code null}.
    * @param index the internal index of the boolean field
    * @return the name of the boolean field or {@code null}
    */
    public String booleanFieldName(final int index) {
      return (booleanFieldIndex > 0) ? boolFields[index].name() : null;
   }

   /**
    * Get the value of the specified string field.  If no string field was set on this
    * {@link Point}, the return value will be {@code null}.
    * @param field the name of the string field
    * @return the string field value or {@code null}
    */
   public String stringField(final String field) {
      for (int i = 0; i < stringFieldIndex; i++) {
         if (field.equals(stringFields[i].name)) {
            return stringFields[i].value;
         }
      }
      return null;
   }

   /**
    * Get the value of the specified String field, by internal index (see {@link #getStringFieldIndex(String)}).
    * If no such String field was set on this {@link Point}, the return value will be {@code null}.
    * @param index the internal index of the String field
    * @return the String field value or {@code null}
    */
    public String stringField(final int index) {
      return (index < stringFieldIndex) ? stringFields[index].value : null;
   }

   /**
    * Get the name of the specified string field, by internal index.  If no such string field was set
    * on this {@link Point}, the return value will be {@code null}.
    * @param index the internal index of the string field
    * @return the name of the string field or {@code null}
    */
    public String stringFieldName(final int index) {
      return (stringFieldIndex > 0) ? stringFields[index].name() : null;
   }

   /**
    * Get the value of the specified tag.  If no such tag was set on this {@link Point},
    * the return value will be {@code null}.
    * @param tag the name of the tag
    * @return the string value or {@code null}
    */
   public String tag(final String tag) {
      for (int i = 0; i < tagIndex; i++) {
         if (tag.equals(tags[i].name)) {
            return tags[i].value;
         }
      }
      return null;
   }

   /**
    * Get the tag (name and value) at the specified index.
    * @param index the internal index of the tag
    * @return an array of two elements, name and value, or {@code null}
    */
   public String[] tag(final int index) {
      if (index >= 0 && index < tagIndex) {
         final StringPair pair = tags[index];
         return new String[] {pair.name, pair.value};
      }
      return null;
   }

   /**
    * Get the type of the specified field.  This call is relatively expensive, so it is
    * recommended that the result be cached if frequent access is neccessary.
    * @param field the name of the field
    * @return the Class of the field, or {@code null}
    */
   public Class<?> getFieldType(final String field) {
      if (longField(field) != null) return Long.class;
      if (doubleField(field) != null) return Double.class;
      if (stringField(field) != null) return String.class;
      if (booleanField(field) != null) return Boolean.class;
      return null;
   }

   /**
    * Get the number of tags currently set on this {@link Point}.
    * @return the number of tags currently set
    */
   public int getTagCount() {
      return tagIndex;
   }

   /**
    * Get the number of fields currently set on this {@link Point}.
    * @return the number of fields currently set
    */
   public int getFieldCount() {
      return longFieldIndex + doubleFieldIndex + booleanFieldIndex + stringFieldIndex;
   }

   /**
    * Get the number of long fields currently set on this {@link Point}.
    * @return the number of long fields currently set
    */
    public int getLongFieldCount() {
      return longFieldIndex;
   }

   /**
    * Get the number of double fields currently set on this {@link Point}.
    * @return the number of double fields currently set
    */
    public int getDoubleFieldCount() {
      return doubleFieldIndex;
   }

   /**
    * Get the number of long fields currently set on this {@link Point}.
    * @return the number of long fields currently set
    */
    public int getStringFieldCount() {
      return stringFieldIndex;
   }

   /**
    * Get the number of long fields currently set on this {@link Point}.
    * @return the number of long fields currently set
    */
    public int getBooleanFieldCount() {
      return booleanFieldIndex;
   }

   /**
    * Get the internal index of the specified long field.  This call is relatively
    * expensive, so it is recommended that the result be cached if frequent access is
    * neccessary.  The index can be used to access the field more efficiently than
    * calling {@link #longField(String)}.
    * @param field the name of the long integer field
    * @return the internal index of the long integer field value or -1
    */
   public int getLongFieldIndex(final String field) {
      for (int i = 0; i < longFieldIndex; i++) {
         if (field.equals(longFields[i].name)) return i;
      }
      return -1;
   }

   /**
    * Get the internal index of the specified double field.  This call is relatively
    * expensive, so it is recommended that the result be cached if frequent access is
    * neccessary.  The index can be used to access the field more efficiently than
    * calling {@link #doubleField(String)}.
    * @param field the name of the double field
    * @return the internal index of the double field value or -1
    */
    public int getDoubleFieldIndex(final String field) {
      for (int i = 0; i < doubleFieldIndex; i++) {
         if (field.equals(doubleFields[i].name)) return i;
      }
      return -1;
   }

   /**
    * Get the internal index of the specified String field.  This call is relatively
    * expensive, so it is recommended that the result be cached if frequent access is
    * neccessary.  The index can be used to access the field more efficiently than
    * calling {@link #stringField(String)}.
    * @param field the name of the String field
    * @return the internal index of the String field value or -1
    */
    public int getStringFieldIndex(final String field) {
      for (int i = 0; i < stringFieldIndex; i++) {
         if (field.equals(stringFields[i].name)) return i;
      }
      return -1;
   }

   /**
    * Get the internal index of the specified boolean field.  This call is relatively
    * expensive, so it is recommended that the result be cached if frequent access is
    * neccessary.  The index can be used to access the field more efficiently than
    * calling {@link #booleanField(String)}.
    * @param field the name of the boolean field
    * @return the internal index of the boolean field value or -1
    */
    public int getBooleanFieldIndex(final String field) {
      for (int i = 0; i < booleanFieldIndex; i++) {
         if (field.equals(boolFields[i].name)) return i;
      }
      return -1;
   }

   /**
    * Get the sequence number of this {@link Point}.  Sequence numbers are unique and
    * monotonically increasing.
    * @return the point sequence number
    */
   public long getSequence() {
      return sequence;
   }

   /**
    * Calling this method will prevent the {@link Point} from returning to the pool of points
    * maintained by the {@link PointFactory} until the {@link #close()} method is called.
    * <p>
    * Each invocation of this method effectively increases an internal "retention count" by one (1).
    * Calling the {@link #close()} method effectively reduces the "retention count" by one (1).  When
    * {@link #close()} is called, and the "retention count" reaches zero (0), the pool will be
    * returned back to the pool of points in the {@link PointFactory}.
    * <p>
    * Every {@link Point} obtained by the {@link PointFactory} has an initial retention count of
    * one (1).  When a point is queued for write by calling {@link InfluxDB#write(Point)}, the
    * {@link InfluxDB} class will automatically call {@link #close()} after the {@link Point} is
    * written.  This method can thus be used to prevent the {@link Point} from being returned to the
    * pool, making the caller of this method responsible for explicitly calling {@link #close()}.
    */
   public void retain() {
      retentionCount.incrementAndGet();
   }

   /**
    * This method is used in conjunction with the {@link #retain()} method to return a {@link Point}
    * back to the pool of points maintained by the {@link PointFactory}.
    * <p>
    * Neither this method, nor the {@link #retain()} method, are needed in typical use scenarios.
    * Only invoke this method if a matching {@link #retain()} call has been made, or if you wish
    * to return the point to the pool <i>without</i> writing it to InfluxDB.
    * @see #retain() for more details.
    */
   @Override
   public void close() {
      final int refs = retentionCount.decrementAndGet();
      if (refs == 0) {
         release();
      }
      else if (refs < 1) {
         throw new IllegalStateException("Unbalanced number of close() calls\n" + this);
      }
   }

   /**
    * Create a copy of this {@link Point}, including its measurement name, tags, and timestamp, but
    * <i>excluding</i> any fields.
    * @return a copy of this {@link Point}, excluding any fields
    */
   public Point copy() {
      return copy(measurement);
   }

   /**
    * Create a copy of this {@link Point}, including its tags and timestamp, but <i>excluding</i> any
    * fields and using the measurement name specified here.
    * @param measurement the measurement name to be used in the copied {@link Point}
    * @return a copy of this {@link Point}, excluding any fields and using the specified measurement name
    */
   public Point copy(final String measurement) {
      final Point copy = parentFactory.createPoint(measurement);

      final int tagCount = tagIndex;
      copy.tagIndex = tagCount;
      for (int i = 0; i < tagCount; i++) {
         copy.tags[i].setPair(tags[i]);
      }

      copy.timestamp = timestamp;
      copy.timeUnit = timeUnit;

      return copy;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      final ByteBuffer buf = ByteBuffer.allocate(MAXIMUM_SERIALIZED_POINT_SIZE);
      write(buf, Precision.MILLISECOND);
      buf.flip();
      return StandardCharsets.UTF_8.decode(buf).toString();
   }

   public String toJson() {
      final StringBuilder sb = new StringBuilder(256)
         .append("{")
         .append("\"measurement\":\"")
         .append(measurement.replace("\"", "\\\""))
         .append("\", \"timestamp\":")
         .append(Precision.MILLISECOND.convert(timestamp, timeUnit))
         .append(", \"tags\": {");
      for (int i = 0; i < tagIndex; i++) {
         sb.append("\"").append(tags[i].name.replaceAll("[^\\w]", "_")).append("\":");
         sb.append("\"");
         escapeForJson(sb, tags[i].value);
         sb.append("\",");
      }
      if (tagIndex > 0) sb.setLength(sb.length() - 1);
      boolean fieldWritten = false;
      sb.append("}, \"fields\": {");
      for (int i = 0; i < stringFieldIndex; i++) {
         sb.append("\"").append(stringFields[i].name.replaceAll("[^\\w]", "_")).append("\":");
         sb.append("\"");
         escapeForJson(sb, stringFields[i].value);
         sb.append("\",");
         fieldWritten = true;
      }
      for (int i = 0; i < longFieldIndex; i++) {
         sb.append("\"").append(longFields[i].name.replaceAll("[^\\w]", "_")).append("\":");
         sb.append(longFields[i].value).append(",");
         fieldWritten = true;
      }
      for (int i = 0; i < doubleFieldIndex; i++) {
         sb.append("\"").append(doubleFields[i].name.replaceAll("[^\\w]", "_")).append("\":");
         sb.append(doubleFields[i].value).append(",");
         fieldWritten = true;
      }
      for (int i = 0; i < booleanFieldIndex; i++) {
         sb.append("\"").append(boolFields[i].name.replaceAll("[^\\w]", "_")).append("\":");
         sb.append(boolFields[i].value).append(",");
         fieldWritten = true;
      }
      if (fieldWritten) sb.setLength(sb.length() - 1);
      sb.append("}");
      sb.append("}");
      return sb.toString();
   }

   void check() throws IllegalStateException {
      final int fieldCount = longFieldIndex + booleanFieldIndex + stringFieldIndex + doubleFieldIndex;

      if (fieldCount == 0) {
         throw new IllegalStateException("Point must have at least one field");
      }
      if (timestamp == 0) {
         throw new IllegalStateException("Point requires a timestamp, default timestamps are not supported");
      }
   }

   void write(final ByteBuffer buffer, final Precision precision) {
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

      boolean firstFieldWritten = false;

      for (int i = 0; i < stringFieldIndex; i++) {
         serializeStringField(buffer, stringFields[i], firstFieldWritten);
         firstFieldWritten = true;
      }

      for (int i = 0; i < longFieldIndex; i++) {
         serializeLongField(buffer, longFields[i], firstFieldWritten);
         firstFieldWritten = true;
      }

      for (int i = 0; i < doubleFieldIndex; i++) {
         serializeDoubleField(buffer, doubleFields[i], firstFieldWritten);
         firstFieldWritten = true;
      }

      for (int i = 0; i < booleanFieldIndex; i++) {
         serializeBooleanField(buffer, boolFields[i], firstFieldWritten);
      }

      serializeTimestamp(buffer, precision.convert(timestamp, timeUnit));

      buffer.put((byte) '\n');
   }

   private void release() {
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
      retentionCount.set(1);

      parentFactory.returnPoint(this);
   }

   private static final String[] HEX_PAD = {"", "000", "00", "0", ""};

   private void escapeForJson(final StringBuilder sb, final String string) {
      boolean needsEscape = false;
      for (int i = 0; i < string.length(); i++) {
         final char c = string.charAt(i);
         if (c < ' ' || c == '"' || c == '\\') {
            needsEscape = true;
            break;
         }
      }

      if (needsEscape) {
         for (int i = 0; i < string.length(); i++) {
            final char c = string.charAt(i);
            switch (c) {
               case '\\':
                  sb.append("\\\\");
                  break;
               case '"':
                  sb.append("\\\"");
                  break;
               case '\b':
                  sb.append("\\b");
                  break;
               case '\f':
                  sb.append("\\f");
                  break;
               case '\n':
                  sb.append("\\n");
                  break;
               case '\r':
                  sb.append("\\r");
                  break;
               case '\t':
                  sb.append("\\t");
                  break;
               default:
                  if (c < ' ') {
                     final String hex = Integer.toHexString(c);
                     final int len = hex.length();
                     sb.append('\\').append('u').append(HEX_PAD[len]).append(hex);
                  }
                  else {
                     sb.append(c);
                  }
                  break;
            }
         }
      }
      else {
         sb.append(string);
      }
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

   private void serializeStringField(final ByteBuffer buffer, final StringPair pair, final boolean firstFieldWritten) {
      addFieldSeparator(buffer, firstFieldWritten);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      buffer.put((byte) '"');
      escapeFieldValue(buffer, pair.value());
      buffer.put((byte) '"');
   }

   private void serializeLongField(final ByteBuffer buffer, final LongPair pair, final boolean firstFieldWritten) {
      addFieldSeparator(buffer, firstFieldWritten);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      writeLongToBuffer(pair.value(), buffer);
      buffer.put((byte) 'i');
   }

   private void serializeDoubleField(final ByteBuffer buffer, final DoublePair pair, final boolean firstFieldWritten) {
      addFieldSeparator(buffer, firstFieldWritten);
      escapeFieldKey(buffer, pair.name());
      buffer.put((byte) '=');
      writeDoubleToBuffer(pair.value(), buffer);
   }

   private void serializeBooleanField(final ByteBuffer buffer, final BooleanPair pair, final boolean firstFieldWritten) {
      addFieldSeparator(buffer, firstFieldWritten);
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

   private static final void addFieldSeparator(final ByteBuffer buffer, final boolean firstFieldWritten) {
      if (firstFieldWritten) {
         buffer.put((byte) ',');
      }
      else {
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

      void setPair(final StringPair other) {
         this.name = other.name;
         this.value = other.value;
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
