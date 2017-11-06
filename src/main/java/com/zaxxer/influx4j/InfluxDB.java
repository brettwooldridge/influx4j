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

import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URLEncoder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

import javax.net.ssl.SSLContext;

import org.jctools.queues.MpscArrayQueue;

import tlschannel.ClientTlsChannel;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;

/**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDB implements AutoCloseable {

   public static enum Protocol {
      HTTP,
      HTTPS,
      UDP;

      @Override
      public String toString() {
         return this.name().toLowerCase();
      }
   }

   public static enum Consistency {
      ALL,
      ANY,
      ONE,
      QUORUM;

      @Override
      public String toString() {
         return this.name().toLowerCase();
      }
   }

   private static final int SNDRCV_BUFFER_SIZE = Integer.getInteger("com.zaxxer.influx4j.sndrcvBufferSize", 1024 * 1024);
   private static final ConcurrentHashMap<String, EncapsulatedConnection> CONNECTIONS = new ConcurrentHashMap<>();

   private final EncapsulatedConnection connection;

   private InfluxDB(final EncapsulatedConnection connection) {
      this.connection = connection;
   }

   public void write(final Point point) {
      connection.write(point);
   }

   @Override
   public void close() {
      connection.close();
   }

   public static Builder builder() {
      return new Builder();
   }

   /**
    * Builder for a {@link InfluxDB} instance.  Call {@link InfluxDB#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private String retentionPolicy;
      private String database;
      private String username;
      private String password;
      private String host = "localhost";
      private int port = 8086;
      private Protocol protocol = Protocol.HTTP;
      private Consistency consistency = Consistency.ONE;
      private SSLContext sslContext;
      private ThreadFactory threadFactory;
      private long autoFlushPeriod = MILLISECONDS(500).toNanos();

      private Builder() {
      }

      public Builder setConnection(final String host, final int port, final Protocol protocol) {
         this.host = host;
         this.port = port;
         this.protocol = protocol;
         return this;
      }

      public Builder setDatabase(final String database) {
         this.database = database;
         return this;
      }

      public Builder setUsername(final String username) {
         this.username = username;
         return this;
      }

      public Builder setPassword(final String password) {
         this.password = password;
         return this;
      }

      public Builder setRetentionPolicy(final String retentionPolicy) {
         this.retentionPolicy = retentionPolicy;
         return this;
      }

      public Builder setConsistency(final Consistency consistency) {
         this.consistency = consistency;
         return this;
      }

      public Builder setAutoFlushPeriod(final long periodMs) {
         if (periodMs < 100L) {
            throw new IllegalArgumentException("autoFlushPeriod must be greater than 100ms");
         }
         this.autoFlushPeriod = MILLISECONDS(periodMs).toNanos();
         return this;
      }

      public Builder setThreadFactory(final ThreadFactory threadFactory) {
         this.threadFactory = threadFactory;
         return this;
      }

      public Builder setSSLContext(final SSLContext sslContext) {
         this.sslContext = sslContext;
         return this;
      }

      public InfluxDB build() {
         EncapsulatedConnection connection;
         try {
            switch (protocol) {
               case HTTP:
               case HTTPS: {
                  connection = CONNECTIONS.computeIfAbsent(protocol + host + port, key -> {createConnection(key);});
                  break;
               }
               case UDP: {
                  final DatagramChannel datagramChannel = DatagramChannel.open();
                  datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, SNDRCV_BUFFER_SIZE);
                  datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, SNDRCV_BUFFER_SIZE);
                  datagramChannel.connect(InetSocketAddress.createUnresolved(host, port));
                  connection = new EncapsulatedConnection(datagramChannel, autoFlushPeriod, null, retentionPolicy, consistency, threadFactory);
                  break;
               }
            }
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }

         return new InfluxDB(connection);
      }

      private EncapsulatedConnection createConnection(final String key) {
         final SocketChannel sockChannel = SocketChannel.open();
         sockChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
         sockChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
         sockChannel.setOption(StandardSocketOptions.SO_SNDBUF, SNDRCV_BUFFER_SIZE);
         sockChannel.setOption(StandardSocketOptions.SO_RCVBUF, SNDRCV_BUFFER_SIZE);
         sockChannel.connect(InetSocketAddress.createUnresolved(host, port));

         GatheringByteChannel channel;
         if (protocol == Protocol.HTTPS) {
            final SSLContext sslContext = (this.sslContext != null) ? this.sslContext : SSLContext.getDefault();
            channel = ClientTlsChannel
               .newBuilder(sockChannel, sslContext)
               .build();
         }
         else {
            channel = sockChannel;
         }

         return new EncapsulatedConnection(channel, autoFlushPeriod, createBaseURL(), retentionPolicy, consistency, threadFactory);
      }

      private String createBaseURL() {
         switch (protocol) {
            case HTTP:
            case HTTPS:
               try {
                  String query = "?db=" + URLEncoder.encode(database, "utf8");
                  if (username != null) {
                     query += "&u=" + URLEncoder.encode(username, "utf8");
                  }
                  if (password != null) {
                     query += "&p=" + URLEncoder.encode(password, "utf8");
                  }
                  if (retentionPolicy != null) {
                     query += "&rp=" + retentionPolicy;
                  }

                  return new URI(protocol.toString(), null, host, port, "/", query, null).toASCIIString();
               }
               catch (final Exception e) {
                  throw new RuntimeException(e);
               }
            default:
               return null;
         }
      }
   }

   private static class EncapsulatedConnection implements Runnable {
      private final GatheringByteChannel channel;
      private final MpscArrayQueue<Point> pointQueue;
      private final Consistency consistency;
      private final String retentionPolicy;
      private final String baseUrl;
      private final long autoFlushPeriod;
      private volatile boolean shutdown;

      EncapsulatedConnection(final GatheringByteChannel channel,
                             final long autoFlushPeriod,
                             final String baseUrl,
                             final String retentionPolicy,
                             final Consistency consistency,
                             final ThreadFactory threadFactory) {
         this.channel = channel;
         this.autoFlushPeriod = autoFlushPeriod;
         this.baseUrl = baseUrl;
         this.retentionPolicy = retentionPolicy;
         this.consistency = consistency;
         this.pointQueue = new MpscArrayQueue<>(64 * 1024);

         final Thread flusher = threadFactory.newThread(this);
         flusher.setDaemon(true);
         flusher.start();
      }

      void write(final Point point) {
         if (!pointQueue.offer(point)) {
            // TODO: queue is full?  force a write?
         }
      }

      void close() {
         try {
            shutdown = true;
            channel.close();
         }
         catch (final IOException e) {
            return;
         }
      }

      @Override
      public void run() {
         int written = 0;
         while (!shutdown) {
            try (final Point point = pointQueue.poll()) {
               if (point != null) {
                  written += point.writeToChannel(channel);
               }
               else {
                  LockSupport.parkNanos(autoFlushPeriod);
               }
            }
            catch (final IOException ioe) {
               // TODO: What? Log? Pretty spammy...
            }
         }
      }
   }
}
