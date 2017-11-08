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

import com.zaxxer.influx4j.util.DaemonThreadFactory;

import stormpot.Allocator;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;

class BufferPoolManager {
   private static final Timeout TIMEOUT = new Timeout(Long.MAX_VALUE, TimeUnit.DAYS);

   private static final BlazePool<PoolableByteBuffer> pool128;
   private static final BlazePool<PoolableByteBuffer> pool512;
   private static final BlazePool<PoolableByteBuffer> pool4096;

   static {
      final DaemonThreadFactory threadFactory = new DaemonThreadFactory();

      final Config<PoolableByteBuffer> config128 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer128Allocator())
         .setSize(4096);
      final Config<PoolableByteBuffer> config512 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer512Allocator())
         .setSize(1024);
      final Config<PoolableByteBuffer> config4096 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer4096Allocator())
         .setSize(128);

      pool128 = new BlazePool<>(config128);
      pool512 = new BlazePool<>(config512);
      pool4096 = new BlazePool<>(config4096);
   }

   static PoolableByteBuffer borrow128Buffer() {
      try {
         final PoolableByteBuffer buffer = pool128.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   static PoolableByteBuffer borrow512Buffer() {
      try {
         final PoolableByteBuffer buffer = pool512.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   static PoolableByteBuffer borrow4096Buffer() {
      try {
         final PoolableByteBuffer buffer = pool4096.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }


   /**
    * An Allocator that allocates 128-byte ByteBuffers.
    */
   private static class ByteBuffer128Allocator implements Allocator<PoolableByteBuffer> {
      @Override
      public PoolableByteBuffer allocate(final Slot slot) throws Exception {
         return new PoolableByteBuffer(slot, 128);
      }

      @Override
      public void deallocate(final PoolableByteBuffer poolable) throws Exception {
         // nothing
      }
   }

   /**
    * An Allocator that allocates 512-byte ByteBuffers.
    */
    private static class ByteBuffer512Allocator implements Allocator<PoolableByteBuffer> {
      @Override
      public PoolableByteBuffer allocate(final Slot slot) throws Exception {
         return new PoolableByteBuffer(slot, 512);
      }

      @Override
      public void deallocate(final PoolableByteBuffer poolable) throws Exception {
         // nothing
      }
   }

   /**
    * An Allocator that allocates 4096-byte ByteBuffers.
    */
    private static class ByteBuffer4096Allocator implements Allocator<PoolableByteBuffer> {
      @Override
      public PoolableByteBuffer allocate(final Slot slot) throws Exception {
         return new PoolableByteBuffer(slot, 4096);
      }

      @Override
      public void deallocate(final PoolableByteBuffer poolable) throws Exception {
         // nothing
      }
   }

   /**
    * An poolable wrapper around a ByteBuffer.
    */
    static class PoolableByteBuffer implements Poolable, AutoCloseable {
      private final Slot slot;
      private final ByteBuffer buffer;

      PoolableByteBuffer(final Slot slot, final int size) {
         this.slot = slot;
         this.buffer = ByteBuffer.allocate(size);
      }

      ByteBuffer getBuffer() {
         return buffer;
      }

      void clear() {
         buffer.clear();
      }

      @Override
      public void close() {
         slot.release(this);
      }

      @Override
      public void release() {
         slot.release(this);
      }
   }
}
