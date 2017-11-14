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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.zaxxer.influx4j.util.DaemonThreadFactory;

import stormpot.Allocator;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.Poolable;
import stormpot.Slot;
import stormpot.Timeout;

class BufferPoolManager implements AutoCloseable {
   private static final Timeout TIMEOUT = new Timeout(Long.MAX_VALUE, TimeUnit.DAYS);

   private final BlazePool<PoolableByteBuffer> pool128;
   private final BlazePool<PoolableByteBuffer> pool512;
   private final BlazePool<PoolableByteBuffer> pool4096;

   private static int leases;
   private static BufferPoolManager bufferPoolManager;

   static synchronized BufferPoolManager leaseBufferPoolManager() {
      if (leases++ == 0) {
         bufferPoolManager = new BufferPoolManager();
      }
      return bufferPoolManager;
   }

   private BufferPoolManager() {
      final DaemonThreadFactory threadFactory = new DaemonThreadFactory("Buffer");

      final Config<PoolableByteBuffer> config128 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer128Allocator())
         .setSize(8192);
      final Config<PoolableByteBuffer> config512 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer512Allocator())
         .setSize(4096);
      final Config<PoolableByteBuffer> config4096 = new Config<>()
         .setThreadFactory(threadFactory)
         .setAllocator(new ByteBuffer4096Allocator())
         .setSize(128);

      pool128 = new BlazePool<>(config128);
      pool512 = new BlazePool<>(config512);
      pool4096 = new BlazePool<>(config4096);
   }

   PoolableByteBuffer borrow128Buffer() {
      try {
         final PoolableByteBuffer buffer = pool128.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   PoolableByteBuffer borrow512Buffer() {
      try {
         final PoolableByteBuffer buffer = pool512.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   PoolableByteBuffer borrow4096Buffer() {
      try {
         final PoolableByteBuffer buffer = pool4096.claim(TIMEOUT);
         buffer.clear();
         return buffer;
      }
      catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public synchronized void close() {
      if (--leases == 0) {
         pool128.shutdown();
         pool512.shutdown();
         pool4096.shutdown();
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
      private final int size;
      private static AtomicInteger claims = new AtomicInteger();

      PoolableByteBuffer(final Slot slot, final int size) {
         this.slot = slot;
         this.size = size;
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
         if (slot != null) {
            slot.release(this);
         }
      }

      @Override
      public void release() {
         if (slot != null) {
            slot.release(this);
         }
      }
   }
}
