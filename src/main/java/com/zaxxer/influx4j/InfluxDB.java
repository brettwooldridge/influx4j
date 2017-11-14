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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.locks.LockSupport;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;

import com.zaxxer.influx4j.BufferPoolManager.PoolableByteBuffer;
import org.jctools.queues.MpscArrayQueue;
import tlschannel.ClientTlsChannel;

import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


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
   private static BufferPoolManager bufferPoolManager;

   private final EncapsulatedConnection connection;

   private InfluxDB(final EncapsulatedConnection connection) {
      this.connection = connection;
   }

   public void write(final Point point) {
      connection.write(point);
   }

   @Override
   public void close() {
      if (connection != null) {
         connection.close();
      }
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
      private String password = "";
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
         if (database == null) throw new IllegalStateException("Influx 'database' must be specified.");
         if (username == null) throw new IllegalStateException("Influx 'username' must be specified.");

         try {
            EncapsulatedConnection connection;
            switch (protocol) {
               case HTTP:
               case HTTPS: {
                  final String url = createBaseURL();
                  if (!validateConnection(createSocketChannel())) {
                     throw new RuntimeException("Access denied to database '" + database + "' for user '" + username + "'.");
                  }

                  connection = CONNECTIONS.computeIfAbsent(url, this::createConnection);
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

      private ByteChannel createSocketChannel() {
         try {
            final SocketChannel sockChannel = SocketChannel.open();
            sockChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            sockChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
            sockChannel.setOption(StandardSocketOptions.SO_SNDBUF, SNDRCV_BUFFER_SIZE);
            sockChannel.setOption(StandardSocketOptions.SO_RCVBUF, SNDRCV_BUFFER_SIZE);
            sockChannel.connect(new InetSocketAddress(host, port));

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

            return channel;
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }

      private EncapsulatedConnection createConnection(final String url) {
         try {
            return new EncapsulatedConnection(url, createSocketChannel(), autoFlushPeriod, threadFactory);
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }

      boolean validateConnection(final ByteChannel byteChannel) throws IOException {
         try (final ByteChannel channel = byteChannel) {
            final ByteBuffer buffer = ByteBuffer.allocate(512);
            buffer.put(("GET " + protocol + "://" + host + ":" + port + "/query?" +
                        "db=" + URLEncoder.encode(database, "utf8") +
                        "&u=" + URLEncoder.encode(username, "utf8") +
                        "&p=" + URLEncoder.encode(password, "utf8") +
                        "&q=" + URLEncoder.encode("SHOW DATABASES", "utf8") +
                        " HTTP/1.1\r\n"
                     ).getBytes());
            buffer.put("Connection: Keep-Alive\r\n".getBytes());
            buffer.put("Host: ".getBytes()).put(InetAddress.getLocalHost().getHostName().getBytes()).put("\r\n\r\n".getBytes());
            buffer.flip();
            channel.write(buffer);

            buffer.clear();
            final int read = channel.read(buffer);
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
            throw new IOException("Unexpected end-of-stream during connection validation");
         }
      }

      private String createBaseURL() {
         try {
            String query = "db=" + URLEncoder.encode(database, "utf8")
               + "&consistency=" + consistency
               + "&precision=" + precision
               + "&rp=" + URLEncoder.encode(retentionPolicy, "utf8");

            if (username != null) {
               query += "&u=" + URLEncoder.encode(username, "utf8");
            }
            if (password != null) {
               query += "&p=" + URLEncoder.encode(password, "utf8");
            }

            return new URI(protocol.toString(), null, host, port, "/write", query, null).toASCIIString();
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   /**
    *
    */
   private static class EncapsulatedConnection implements Runnable {
      private final ByteChannel channel;
      private final MpscArrayQueue<Point> pointQueue;
      private final ArrayList<PoolableByteBuffer> pendingQueue;
      private final PoolableByteBuffer httpHeaders;
      private final String url;
      private final long autoFlushPeriod;
      private final int contentLengthOffset;
      private volatile boolean shutdown;

      EncapsulatedConnection(final String url,
                             final ByteChannel channel,
                             final long autoFlushPeriod,
                             final ThreadFactory threadFactory) throws IOException {
         this.url = url;
         this.channel = channel;
         this.autoFlushPeriod = autoFlushPeriod;
         this.pointQueue = new MpscArrayQueue<>(64 * 1024);
         this.pendingQueue = new ArrayList<>(512);
         this.httpHeaders = new PoolableByteBuffer(null, 512);
         this.contentLengthOffset = setupHttpHeaderBuffer();

         final Thread flusher = threadFactory.newThread(this);
         flusher.setDaemon(true);
         flusher.start();
      }

      void write(final Point point) {
         if (!pointQueue.offer(point)) {
            throw new RuntimeException(System.currentTimeMillis() + " Point queue overflow.  Exceeded capacity of " + pointQueue.capacity() + ".");
         }
      }

      void close() {
         if (shutdown) return;

         try {
            shutdown = true;
            channel.close();
         }
         catch (final IOException e) {
            return;
         }
         finally {
            CONNECTIONS.remove(url);
         }
      }

      @Override
      public void run() {
         int bytesPending = 0;
         while (!shutdown) {
            final long startNs = nanoTime();
            do {
               if (pendingQueue.isEmpty()) {
                  pendingQueue.add(httpHeaders);
               }

               try (final Point point = pointQueue.poll()) {
                  if (point == null) break;
                  bytesPending += point.enqueueBuffers(pendingQueue);
               }
            } while (bytesPending < SNDRCV_BUFFER_SIZE - (64 * 1024) && !shutdown);

            if (pendingQueue.size() > 1) {
               writeBuffers(bytesPending);
               bytesPending = 0;
            }

            final long parkTime = autoFlushPeriod - (nanoTime() - startNs);
            if (parkTime > 0) {
               LockSupport.parkNanos(parkTime);
            }
         }
      }

      private int setupHttpHeaderBuffer() throws UnknownHostException {
         final ByteBuffer buffer = httpHeaders.getBuffer();
         buffer.put(("POST " + url + " HTTP/1.1\r\n").getBytes());
         buffer.put(("Host: " + InetAddress.getLocalHost().getHostName() + "\r\n").getBytes());
         buffer.put("Content-Type: application/x-www-form-urlencoded\r\n".getBytes());
         buffer.put("Content-Length: ".getBytes());
         return buffer.position();
      }

      private void writeBuffers(final int bytesPending) {
         final ByteBuffer contentLengthBuffer = httpHeaders.getBuffer();
         writeLongToBuffer(bytesPending, contentLengthBuffer);
         contentLengthBuffer.put((byte) '\r').put((byte) '\n').put((byte) '\r').put((byte) '\n').flip();

         // Cache these in local variables to avoid member dereferencing
         final GatheringByteChannel byteChannel = (GatheringByteChannel) channel;
         final ArrayList<PoolableByteBuffer> buffers = pendingQueue;

         try {
            final int bufferCount = buffers.size();
            for (int i = 0; i < bufferCount; i++) {
               // Using try-with-resources releases the PoolableByteBuffer after writing
               try (final PoolableByteBuffer buffer = buffers.get(i)) {
                  byteChannel.write(buffer.getBuffer());
               }
            }

            final String response = readResponse(channel);
            final int status = Integer.valueOf(response.substring(9, 12));
            if (status == 401) {
               // re-authenticate?
            }
            else if (status > 399) {
               System.err.println(response);
               // unexpected response
            }
         }
         catch (final IOException io) {
            // TODO: What? Log? Pretty spammy...
            io.printStackTrace();
         }

         // Clear the pending queue
         pendingQueue.clear();
         // Reset the http header buffer to the point where the next Content-Length value will be written
         contentLengthBuffer.clear().position(contentLengthOffset);
      }
   }

   private static String readResponse(final ByteChannel channel) throws IOException {
      try (final BufferPoolManager bufferPool = BufferPoolManager.leaseBufferPoolManager();
           final PoolableByteBuffer pbb = bufferPool.borrow512Buffer()) {
         final ByteBuffer buffer = pbb.getBuffer();
         do {
            final int read = channel.read(buffer);
            if (read < 0) {
               throw new IOException("Unexpected end-of-stream");
            }
         } while (buffer.position() < 12);

         // HTTP/1.1 xxx
         buffer.flip();
         byte[] bytes = buffer.array();
         return new String(bytes, 0, buffer.remaining());
      }
   }
}
