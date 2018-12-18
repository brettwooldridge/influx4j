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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.zaxxer.influx4j.util.FAAArrayQueue;

 /**
  * This class provides the source of {@link Point} instances to be persisted by
  * calling {@link InfluxDB#write(Point)}.
  *
  * @author brett.wooldridge at gmail.com
  */
public class PointFactory {
   private final FAAArrayQueue<Point> pointPool;
   private final AtomicLong sequence;
   private final int maxPoolSize;

   /**
    * Obtain a new {@link PointFactory.Builder} instance to configure and create
    * a {@link PointFactory}.
    *
    * @return a new {@link PointFactory.Builder} instance
    */
   public static Builder builder() {
      return new Builder();
   }

   /**
    * Create a {@link Point} with the specified {@code measurement} name.
    * <p>
    * This method is <i>thread-safe</i> and may safely be called by multiple threads
    * concurrently.
    *
    * @param measurement the measurement name
    * @return a new {@link Point} instance, likely obtained from the internal pool
    */
   public Point createPoint(final String measurement) {
      Point point = pointPool.dequeue();
      if (point == null) {
         point = new Point(this);
      }

      point.measurement(measurement);
      point.sequence = sequence.getAndIncrement();
      return point;
   }

   void returnPoint(final Point point) {
      if (pointPool.size() < maxPoolSize) {
         pointPool.enqueue(point);
      }

      // otherwise, just allow the Point to be garbage collected
   }

   /**
    * Flush all {@link Point} instances from the internal pool.  Use of this method
    * is not generally necessary nor recommended due to the impact on performance
    * as well as garbage generation side-effect.
    * <p>
    * This method is <i>thread-safe</i> and may safely be called by multiple threads or
    * while other threads are calling {@link #createPoint(String)}.
    */
   public void flush() {
      while (pointPool.dequeue() != null);
   }

   private PointFactory(final int initialPoolSize, final int maxPoolSize) {
      this.pointPool = new FAAArrayQueue<>();
      this.maxPoolSize = maxPoolSize;
      this.sequence = new AtomicLong();

      // Pre-populate the pool
      for (int i = 0; i < initialPoolSize; i++) {
         pointPool.enqueue(new Point(this));
      }
   }

   /**
    * Builder for a {@link PointFactory} instance.  Call {@link PointFactory#builder()} to
    * create an instance of the {@link PointFactory.Builder}.
    */
   public static class Builder {
      private int size = 128;
      private int maxSize = 512;

      private Builder() {
      }

      /**
       * Sets the <i>initial</i> size of the internal {@link Point} pool.
       * @param size the initial size of the internal pool
       * @return this {@link PointFactory.Builder}
       */
      public Builder initialSize(final int size) {
         this.size = size;
         return this;
      }

      /**
       * Sets the <i>maximum</i> size of the internal {@link Point} pool.
       * @param size the maximum size of the internal pool
       * @return this {@link PointFactory.Builder}
       */
      public Builder maximumSize(final int size) {
         this.maxSize = size;
         return this;
      }

      public PointFactory build() {
          return new PointFactory(size, Math.max(size, maxSize));
      }
   }
}
