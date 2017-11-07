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
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.jctools.queues.MpscArrayQueue;
import tlschannel.ClientTlsChannel;


/**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDB implements AutoCloseable {

   /**
    * InfluxDB wire protocols.
    */
   public static enum Protocol {
      HTTP,
      HTTPS,
      UDP;

      @Override
      public String toString() {
         return this.name().toLowerCase();
      }
   }

   /**
    * InfluxDB data consistency.
    */
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

   /**
    * InfluxDB timestamp precision.
    */
   public static enum Precision {
      NANOSECOND("n"),
      MICROSECOND("u"),
      MILLISECOND("ms"),
      SECOND("s"),
      MINUTE("m"),
      HOUR("h");

      private final String precision;

      Precision(final String p) {
         this.precision = p;
      }

      @Override
      public String toString() {
         return precision;
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


   /** ***************************************************************************************
    * Builder for a {@link InfluxDB} instance.  Call {@link InfluxDB#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private String retentionPolicy = "autogen";
      private String database;
      private String username;
      private String password;
      private String host = "localhost";
      private int port = 8086;
      private long autoFlushPeriod = MILLISECONDS.toNanos(500);
      private Protocol protocol = Protocol.HTTP;
      private Consistency consistency = Consistency.ONE;
      private Precision precision = Precision.NANOSECOND;
      private SSLContext sslContext;
      private ThreadFactory threadFactory;

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

      public Builder setPrecision(final Precision precision) {
         this.precision = precision;
         return this;
      }

      public Builder setAutoFlushPeriod(final long periodMs) {
         if (periodMs < 100L) {
            throw new IllegalArgumentException("autoFlushPeriod must be greater than 100ms");
         }
         this.autoFlushPeriod = MILLISECONDS.toNanos(periodMs);
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
         try {
            EncapsulatedConnection connection;
            switch (protocol) {
               case HTTP:
               case HTTPS: {
                  final String url = createBaseURL();
                  connection = CONNECTIONS.computeIfAbsent(url, this::createConnection);
                  if (!validateConnection(connection)) {
                     throw new RuntimeException("Access denied to database '" + database + "' for user '" + username + "'.");
                  }
                  break;
               }
               case UDP: {
                  final DatagramChannel datagramChannel = DatagramChannel.open();
                  datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, SNDRCV_BUFFER_SIZE);
                  datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, SNDRCV_BUFFER_SIZE);
                  datagramChannel.connect(InetSocketAddress.createUnresolved(host, port));
                  connection = new EncapsulatedConnection(null, datagramChannel, autoFlushPeriod, threadFactory);
                  break;
               }
               default:
                  throw new IllegalArgumentException("Unknown protocol: " + protocol);
            }

            return new InfluxDB(connection);
         }
         catch (final IOException e) {
            throw new RuntimeException(e);
         }
      }

      private EncapsulatedConnection createConnection(final String url) {
         try {
            final SocketChannel sockChannel = SocketChannel.open();
            sockChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            sockChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            sockChannel.setOption(StandardSocketOptions.SO_SNDBUF, SNDRCV_BUFFER_SIZE);
            sockChannel.setOption(StandardSocketOptions.SO_RCVBUF, SNDRCV_BUFFER_SIZE);
            sockChannel.connect(InetSocketAddress.createUnresolved(host, port));

            ByteChannel channel;
            if (protocol == Protocol.HTTPS) {
               final SSLContext sslContext = (this.sslContext != null) ? this.sslContext : SSLContext.getDefault();
               channel = ClientTlsChannel
                  .newBuilder(sockChannel, sslContext)
                  .build();
            }
            else {
               channel = sockChannel;
            }

            return new EncapsulatedConnection(url, channel, autoFlushPeriod, threadFactory);
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }

      boolean validateConnection(final EncapsulatedConnection connection) throws IOException {
         final ByteBuffer buffer = ByteBuffer.allocate(512);
         buffer.put(("GET " + protocol + "://" + host + ":" + port + "/query?" +
                     "db=" + URLEncoder.encode(database, "utf8") +
                     "&u=" + URLEncoder.encode(username, "utf8") +
                     "&p=" + URLEncoder.encode(password, "utf8") +
                     "&q=" + URLEncoder.encode("SHOW GRANTS FOR \"" + username + "\"", "utf8") +
                     " HTTP/1.1\r\n"
                    ).getBytes());
         buffer.put("Connection: Keep-Alive\r\n\r\n".getBytes());
         buffer.flip();
         connection.channel.write(buffer);

         buffer.clear();
         final int read = connection.channel.read(buffer);
         if (read > 0) {
            buffer.flip();
            final String response = new String(buffer.array(), 0, read);
            if (response.startsWith("HTTP/1.1 2")) {
               return true;
            }
            else if (response.startsWith("HTTP/1.1 401")) {
               return false;
            }
         }
         throw new IOException("Unexpected response during connection validation: " + response);
      }

      private String createBaseURL() {
         try {
            String query = "POST /write/?db=" + URLEncoder.encode(database, "utf8")
               + "&consistency=" + consistency
               + "&precision=" + precision
               + "&rp=" + URLEncoder.encode(retentionPolicy, "utf8");

            if (username != null) {
               query += "&u=" + URLEncoder.encode(username, "utf8");
            }
            if (password != null) {
               query += "&p=" + URLEncoder.encode(password, "utf8");
            }

            return new URI(protocol.toString(), null, host, port, "/", query, null).toASCIIString();
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   private static class EncapsulatedConnection implements Runnable {
      private final ByteChannel channel;
      private final MpscArrayQueue<Point> pointQueue;
      private final String url;
      private final long autoFlushPeriod;
      private volatile boolean shutdown;

      EncapsulatedConnection(final String url,
                             final ByteChannel channel,
                             final long autoFlushPeriod,
                             final ThreadFactory threadFactory) {
         this.url = url;
         this.channel = channel;
         this.autoFlushPeriod = autoFlushPeriod;
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
         final GatheringByteChannel byteChannel = (GatheringByteChannel) channel;
         int written = 0;
         while (!shutdown) {
            do {
               try (final Point point = pointQueue.poll()) {
                  if (point == null) break;
                  if (written == 0) {
                     writeHttpRequestHeaders();
                  }

                  written += point.writeToChannel(byteChannel);
               }   
            } while (!shutdown && written < SNDRCV_BUFFER_SIZE - (64 * 1024));

            if (written > 0) {
               // 
            }
                  LockSupport.parkNanos(autoFlushPeriod);

            }
            catch (final IOException ioe) {
               // TODO: What? Log? Pretty spammy...
            }
         }
      }

      private void writeHttpRequestHeaders() {
         final ByteBuffer buffer = ByteBuffer.allocate(512);
         buffer.put("POST  ")
         channel.write(src)
      }
   }
}
