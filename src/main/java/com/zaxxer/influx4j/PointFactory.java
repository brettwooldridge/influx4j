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

import com.zaxxer.influx4j.util.FAAArrayQueue;

 /**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("WeakerAccess")
public class PointFactory {
   private final FAAArrayQueue<Point> pointPool;

   public static Builder builder() {
      return new Builder();
   }

   public Point createPoint(final String measurement) {
      Point point = pointPool.dequeue();
      if (point == null) {
         point = new Point(this);
      }

      point.measurement(measurement);
      return point;
   }

   void returnPoint(final Point point) {
      pointPool.enqueue(point);
   }

   public void close() {
      while (pointPool.dequeue() != null);
   }

   private PointFactory(final int initialPoolSize) {
      this.pointPool = new FAAArrayQueue<>();

      // Pre-populate the pool
      for (int i = 0; i < initialPoolSize; i++) {
         pointPool.enqueue(new Point(this));
      }
   }

   /**
    * Builder for a {@link PointFactory} instance.  Call {@link PointFactory#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private int size = 128;

      private Builder() {
      }

      public Builder setSize(final int size) {
         this.size = size;
         return this;
      }

      public PointFactory build() {
          return new PointFactory(size);
      }
   }
}
