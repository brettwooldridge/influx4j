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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import javax.net.ssl.SSLContext;

import org.jctools.queues.MpscArrayQueue;

import com.zaxxer.influx4j.util.DaemonThreadFactory;
import com.zaxxer.influx4j.util.HttpElf;

import tlschannel.ClientTlsChannel;
import tlschannel.TlsChannel;

import static com.zaxxer.influx4j.util.FastValue2Buffer.writeLongToBuffer;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDB implements AutoCloseable {

   /*****************************************************************************************
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

   /*****************************************************************************************
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

   /*****************************************************************************************
    * InfluxDB timestamp precision.
    */
   public static enum Precision {
      NANOSECOND("n", TimeUnit.NANOSECONDS),
      MICROSECOND("u", TimeUnit.MICROSECONDS),
      MILLISECOND("ms", TimeUnit.MILLISECONDS),
      SECOND("s", TimeUnit.SECONDS),
      MINUTE("m", TimeUnit.MINUTES),
      HOUR("h", TimeUnit.HOURS);

      private final String precision;
      private final TimeUnit converter;

      Precision(final String p, final TimeUnit converter) {
         this.precision = p;
         this.converter = converter;
      }

      long convert(final long t, final TimeUnit sourceUnit) {
         return converter.convert(t, sourceUnit);
      }

      @Override
      public String toString() {
         return precision;
      }
   }


   private static final int SNDRCV_BUFFER_SIZE = Integer.getInteger("com.zaxxer.influx4j.sndrcvBufferSize", 1024 * 1024);
   private static final int MAXIMUM_SERIALIZED_POINT_SIZE = Integer.getInteger("com.zaxxer.influx4j.maxSerializedPointSize", 32 * 1024);

   private static final ConcurrentHashMap<String, SocketConnection> CONNECTIONS = new ConcurrentHashMap<>();

   private final SocketConnection connection;
   private final String host;
   private final String username;
   private final String password;
   private final int port;
   private final Protocol protocol;

   private InfluxDB(final SocketConnection connection,
                    final String host,
                    final int port,
                    final Protocol protocol,
                    final String username,
                    final String password) {
      this.connection = connection;
      this.host = host;
      this.port = port;
      this.protocol = protocol;
      this.username = username;
      this.password = password;
   }

   /*****************************************************************************************
    * InfluxDB public methods.
    */

   /**
    * Write a {@link Point} to the database.  If the HTTP/S protocol is used, points are buffered
    * and flushed at the interval specified by the {@link Builder#autoFlushPeriod} (1 second default).
    * If the UDP protocol is used, points are not buffered and are written immediately to the
    * database.
    *
    * @param point the point to write to the database
    */
   public void write(final Point point) {
      connection.write(point);
   }

   /**
    * Close the connection to the database.
    */
   @Override
   public void close() {
      if (connection != null) {
         connection.close();
      }
   }

   public String createDatabase(final String name) {
      return createDatabase(name, host, port, protocol, username, password);
   }

   /**
    * Get an instance of an InfluxDB {@link Builder}.
    */
   public static Builder builder() {
      return new Builder();
   }


   private static String createDatabase(final String name,
                                        final String host,
                                        final int port,
                                        final Protocol protocol,
                                        final String username,
                                        final String password) {
      try {
         final String query = "db="
            + "&u=" + URLEncoder.encode(username, "utf8")
            + "&p=" + URLEncoder.encode(password, "utf8")
            + "&q=create+database+" + URLEncoder.encode(name, "utf8");

            final URI uri = new URI(protocol.toString(), null, host, port, "/query", query, null);
            final HttpURLConnection httpConnection = (HttpURLConnection) uri.toURL().openConnection();
            httpConnection.setConnectTimeout((int) SECONDS.toMillis(5));
            httpConnection.setRequestMethod("POST");
            httpConnection.setRequestProperty("Host", InetAddress.getLocalHost().getHostName());
            httpConnection.setRequestProperty("Content-Length", "0");

            final InputStream is = httpConnection.getInputStream();
            final byte[] buffer = new byte[512];
            final int read = is.read(buffer);
            if (read < 12) {
               final String response = new String(buffer, 0, read);
               final int status = Integer.valueOf(response.substring(9, 12));
               if (status > 299) {
                  return response;
               }

               return null;
            }
            else {
               return "Unexpected end-of-stream";
            }
      }
      catch (final Exception e) {
         return e.getMessage();
      }
   }

   /*****************************************************************************************
    * Builder for an {@link InfluxDB} instance.  Call {@link InfluxDB#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private String retentionPolicy = "autogen";
      private String database;
      private String username;
      private String password = "";
      private String host = "localhost";
      private int port = 8086;
      private long autoFlushPeriod = SECONDS.toNanos(1);
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

      public InfluxDB create() {
         // Errors are ignored except for those that actually throw from createDatabase().
         createDatabase(database, host, port, protocol, username, password);

         return build();
      }

      public InfluxDB build() {
         if (database == null) throw new IllegalStateException("Influx 'database' must be specified.");
         if (username == null) throw new IllegalStateException("Influx 'username' must be specified.");
         if (threadFactory == null) threadFactory = new DaemonThreadFactory("InfluxDB flusher " + host + ":" + port + "-" + database);

         try {
            SocketConnection connection = null;
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
                  //connection = new SocketConnection(null, datagramChannel, autoFlushPeriod, threadFactory);
                  break;
               }
               default:
                  throw new IllegalArgumentException("Unknown protocol: " + protocol);
            }

            return new InfluxDB(connection, host, port, protocol, username, password);
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
            sockChannel.configureBlocking(false);

            ByteChannel channel = null;
            if (protocol == Protocol.HTTPS) {
               final SSLContext sslContext = (this.sslContext != null) ? this.sslContext : SSLContext.getDefault();
               channel = ClientTlsChannel.newBuilder(sockChannel, sslContext).build();
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

      private SocketConnection createConnection(final String url) {
         try {
            return new SocketConnection(url, createSocketChannel(), precision, autoFlushPeriod, threadFactory);
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
            buffer.put("Connection: Keep-Alive\r\n".getBytes()).put("Host: ".getBytes()).put(InetAddress.getLocalHost().getHostName().getBytes()).put("\r\n\r\n".getBytes());

            buffer.flip();
            channel.write(buffer);
            buffer.clear();

            final String response = HttpElf.readResponse((SocketChannel) byteChannel, buffer);
            if (response.startsWith("HTTP/1.1 2")) {
               return true;
            }
            else if (response.startsWith("HTTP/1.1 401")) {
               return false;
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


   /*****************************************************************************************
    * SocketConnection is used for HTTP/S protocol interactions.
    */
   private static class SocketConnection implements Runnable {
      private final static int HTTP_HEADER_BUFFER_SIZE = 512;
      private final Semaphore shutdownSemaphore;
      private final SocketChannel channel;
      private final Precision precision;
      private final MpscArrayQueue<Point> pointQueue;
      private final ByteBuffer httpHeaders;
      private final String url;
      private final long autoFlushPeriod;
      private final int contentLengthOffset;
      private volatile boolean shutdown;

      SocketConnection(final String url,
                             final ByteChannel channel,
                             final Precision precision,
                             final long autoFlushPeriod,
                             final ThreadFactory threadFactory) throws IOException {
         this.url = url;
         this.channel = (channel instanceof SocketChannel ? (SocketChannel) channel : (SocketChannel) ((TlsChannel) channel).getUnderlying());
         this.precision = precision;
         this.autoFlushPeriod = autoFlushPeriod;
         this.pointQueue = new MpscArrayQueue<>(64 * 1024);
         this.httpHeaders = ByteBuffer.allocate(HTTP_HEADER_BUFFER_SIZE);
         this.contentLengthOffset = setupHttpHeaderBuffer();
         this.shutdownSemaphore = new Semaphore(1);
         this.shutdownSemaphore.acquireUninterruptibly();

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
            CONNECTIONS.remove(url);
            shutdownSemaphore.acquire();
         }
         catch (final InterruptedException e) {
            // just exit
         }
      }

      @Override
      public void run() {
         final ByteBuffer buffer = ByteBuffer.allocate(SNDRCV_BUFFER_SIZE - HTTP_HEADER_BUFFER_SIZE);
         try {
            while (!shutdown) {
               final long startNs = nanoTime();

               if (buffer.position() == 0) {
                  httpHeaders.flip();
                  buffer.put(httpHeaders);
               }

               do {
                  final Point point = pointQueue.poll();
                  if (point == null) break;
                  try {
                     point.write(buffer, precision);
                  }
                  finally {
                     point.release();
                  }
               } while (buffer.remaining() > MAXIMUM_SERIALIZED_POINT_SIZE);

               if (buffer.position() > httpHeaders.limit()) {
                  writeBuffers(buffer);
                  continue;
               }

               final long parkNs = autoFlushPeriod - (nanoTime() - startNs);
               if (parkNs > 10000L) {
                  LockSupport.parkNanos(parkNs);
               }
            }
         }
         catch (final Exception e) {
            e.printStackTrace();
         }
         finally {
            try {
               channel.close();
            }
            catch (IOException io) {
               io.printStackTrace();
               // ignored
            }
            finally {
               shutdownSemaphore.release();
            }
         }
      }

      /**
       * Setup the static HTTP POST header, and return the position in the buffer immediately
       * after the "Content-Length: " header.
       */
      private int setupHttpHeaderBuffer() throws UnknownHostException {
         final int offset = httpHeaders
            .put(("POST " + url + " HTTP/1.1\r\n").getBytes())
            .put(("Host: " + InetAddress.getLocalHost().getHostName() + "\r\n").getBytes())
            .put("Content-Type: application/x-www-form-urlencoded\r\n".getBytes())
            .put("Content-Length: ".getBytes())
            .position();

         httpHeaders.put("00000000\r\n\r\n".getBytes());
         return offset;
      }

      private void writeBuffers(final ByteBuffer buffer) {
         // Capture the end of buffer position
         final int endOfBufferPosition = buffer.position();
         // Set the position to just after the Content-Length header
         buffer.position(contentLengthOffset);
         // Write 8 zeroes for the size, plus the final CRLF+CRLF of the HTTP header
         buffer.put("00000000\r\n\r\n".getBytes());
         // Calculate the content length as the end-of-buffer position minus the HTTP header length
         final int contentLength = endOfBufferPosition - buffer.position();
         // Set the position such that the 8 zeros will be partially overwritten by the content length,
         // resulting in a length with leading zeros, for example, 00000482.
         buffer.position(contentLengthOffset + (8 - String.valueOf(contentLength).length()));
         // Write the content length into the buffer (overwriting some of the zeros)
         writeLongToBuffer(contentLength, buffer);
         // Restore position to end of the buffer
         buffer.position(endOfBufferPosition);

         try {
            buffer.flip();
            while (channel.isConnected()) {
               channel.write(buffer);
               if (buffer.remaining() > 0) {
                  LockSupport.parkNanos(10000L);
                  continue;
               }

               break;
            }
            buffer.clear();

            final String response = HttpElf.readResponse(channel, buffer);
            final int status = Integer.valueOf(response.substring(9, 13).trim());
            if (status == 401) {
               // re-authenticate?
            }
            else if (status > 399) {
               // unexpected response
               throw new RuntimeException("Unexpected HTTP response status: " + status);
            }
         }
         catch (final IOException io) {
            // TODO: What? Log? Pretty spammy...
            io.printStackTrace();
         }
         finally {
            buffer.clear();
         }
      }
   }
}
