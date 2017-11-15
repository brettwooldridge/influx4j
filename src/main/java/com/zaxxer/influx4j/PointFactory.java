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

import stormpot.Allocator;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.Slot;
import stormpot.Timeout;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

 /**
 * @author brett.wooldridge at gmail.com
 */
@SuppressWarnings("WeakerAccess")
public class PointFactory {
   private static final Timeout TIMEOUT = new Timeout(Long.MAX_VALUE, TimeUnit.DAYS);

   private final BlazePool<Point> pointPool;

   public static Builder builder() {
      return new Builder();
   }

   public Point createPoint(final String measurement) {
      try {
         final Point point = pointPool.claim(TIMEOUT);
         point.measurement(measurement);
         return point;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   public void close() {
      pointPool.shutdown();
   }

   private PointFactory(final Config<Point> config) {
      this.pointPool = new BlazePool<>(config);
      this.pointPool.setTargetSize(512);
   }

   /**
    * Builder for a {@link PointFactory} instance.  Call {@link PointFactory#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private final Config<Point> config;

      private Builder() {
         config = new Config<Point>().setSize(512);
      }

      public Builder setSize(final int size) {
         config.setSize(size);
         return this;
      }

      public Builder setThreadFactory(final ThreadFactory threadFactory) {
         config.setThreadFactory(threadFactory);
         return this;
      }

      public PointFactory build() {
          config.setAllocator(new PointAllocator());
          final PointFactory pointFactory = new PointFactory(config);
          return pointFactory;
      }
   }


   /**
    * {@code Allocator} used by StormPot for managing poolable object lifetimes.
    */
   private static class PointAllocator implements Allocator<Point> {
      @Override
      public Point allocate(final Slot slot) throws Exception {
         return new Point(slot);
      }

      @Override
      public void deallocate(final Point poolable) throws Exception {
         // nothing
      }
   }
}
