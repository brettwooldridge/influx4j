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

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.zaxxer.influx4j.util.DaemonThreadFactory;

import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;

import org.jctools.queues.MpscArrayQueue;


/**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDB implements AutoCloseable {

   private static final Logger LOGGER = Logger.getLogger(InfluxDB.class.getName());

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

   public static final int MAXIMUM_SERIALIZED_POINT_SIZE = Integer.getInteger("com.zaxxer.influx4j.maxSerializedPointSize", 32 * 1024);
   private static final int SEND_BUFFER_SIZE;

   private static final ConcurrentHashMap<URL, SocketConnection> CONNECTIONS = new ConcurrentHashMap<>();

   private final SocketConnection connection;
   private final String host;
   private final String username;
   private final String password;
   private final int port;
   private final Protocol protocol;

   static {
      int sendBuffSize = Integer.getInteger("com.zaxxer.influx4j.sndrcvBufferSize", 0);
      try (final Socket tmpSocket = new Socket()) {
         if (sendBuffSize == 0) {
            sendBuffSize = tmpSocket.getSendBufferSize();
         }
      }
      catch (final IOException ioe) {
         // nothing
      }
      finally {
         SEND_BUFFER_SIZE = sendBuffSize;
      }
   }

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

            final URL uri = new URL(protocol.toString(), host, port, "/query?" + query);
            final HttpURLConnection httpConnection = (HttpURLConnection) uri.openConnection();
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
                  if (!validateConnection()) {
                     throw new RuntimeException("Access denied to database '" + database + "' for user '" + username + "'.");
                  }

                  connection = CONNECTIONS.computeIfAbsent(
                     createBaseURL("/write",
                                   "consistency=" + consistency,
                                   "precision=" + precision,
                                   "rp=" + URLEncoder.encode(retentionPolicy, "utf8")),
                                   this::createConnection);
                  break;
               }
               case UDP: {
                  // final DatagramChannel datagramChannel = DatagramChannel.open();
                  // datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, SEND_BUFFER_SIZE);
                  // datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, SEND_BUFFER_SIZE);
                  // datagramChannel.connect(InetSocketAddress.createUnresolved(host, port));
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

      private SocketConnection createConnection(final URL url) {
         try {
            return new SocketConnection(url, precision, autoFlushPeriod, threadFactory);
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }

      boolean validateConnection() throws IOException {
         final URL url = createBaseURL("/query", "q=" + URLEncoder.encode("SHOW DATABASES", "utf8"));
         final HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();
         httpConnection.setReadTimeout((int) SECONDS.toMillis(5));
         httpConnection.setConnectTimeout((int) SECONDS.toMillis(5));
         httpConnection.setRequestProperty("Host", InetAddress.getLocalHost().getHostName());

         httpConnection.getContent();
         final int status = httpConnection.getResponseCode();
         if (status < 300) {
            return true;
         }
         else if (status == 401) {
            return false;
         }
         throw new IOException("Unexpected end-of-stream during connection validation");
      }

      private URL createBaseURL(final String path, final String... queryParameters) {
         try {
            String query =
               "db=" + URLEncoder.encode(database, "utf8") +
               "&u=" + (username != null ? URLEncoder.encode(username, "utf8") : "") +
               "&p=" + (password != null ? URLEncoder.encode(password, "utf8") : "");

            query = query + "&" + String.join("&", queryParameters);

            return new URL(protocol.toString(), host, port, path + "?" + query);
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
      private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");

      private final OkHttpClient client;
      private final String localhost = InetAddress.getLocalHost().getHostName();
      private final Semaphore shutdownSemaphore;
      private final Precision precision;
      private final MpscArrayQueue<Point> pointQueue;
      private final URL url;
      private final long autoFlushPeriod;
      private volatile boolean shutdown;

      SocketConnection(final URL url,
                       final Precision precision,
                       final long autoFlushPeriod,
                       final ThreadFactory threadFactory) throws IOException {
         this.url = url;
         this.precision = precision;
         this.autoFlushPeriod = autoFlushPeriod;
         this.pointQueue = new MpscArrayQueue<>(64 * 1024);
         this.shutdownSemaphore = new Semaphore(1);
         this.shutdownSemaphore.acquireUninterruptibly();
         this.client = new OkHttpClient();

         final Thread flusher = threadFactory.newThread(this);
         flusher.setDaemon(true);
         flusher.start();
      }

      void write(final Point point) {
         if (!pointQueue.offer(point)) {
            LOGGER.log(Level.FINE, "Point queue overflow.  Exceeded capacity of {}, point was dropped.", pointQueue.capacity());
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
         final ByteBuffer buffer = ByteBuffer.allocate(SEND_BUFFER_SIZE - 512);

         final RequestBody requestBody = new RequestBody() {
            @Override public MediaType contentType() {
               return MEDIA_TYPE_TEXT;
             }

            @Override public void writeTo(BufferedSink sink) throws IOException {
               try {
                  buffer.flip();
                  sink.write(buffer.array(), 0, buffer.remaining());
               }
               finally {
                  buffer.clear();
               }
            }
         };

         final Request request = new Request.Builder()
            .url(url)
            .post(requestBody)
            .build();

         final Call httpCall = client.newCall(request);

         try {
            while (!shutdown) {
               final long startNs = nanoTime();

               do {
                  try (final Point point = pointQueue.poll()) {
                     if (point == null) break;

                     point.write(buffer, precision);
                  }
               } while (buffer.remaining() >= MAXIMUM_SERIALIZED_POINT_SIZE);

               if (buffer.position() > 0) {
                  final boolean again = buffer.remaining() < MAXIMUM_SERIALIZED_POINT_SIZE;
                  writeBuffers(httpCall);
                  if (again) {
                     // skip parking below, we still have more points to process but just ran out of buffer
                     continue;
                  }
               }

               final long parkNs = autoFlushPeriod - (nanoTime() - startNs);
               if (parkNs > 10000L) {
                  LockSupport.parkNanos(parkNs);
               }
            }
         }
         catch (final Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected exception", e);
         }
         finally {
            shutdownSemaphore.release();
         }
      }

      private void writeBuffers(final Call httpCall) {
         final Call call = httpCall.clone();
         try (Response response = call.execute()) {
            if (!response.isSuccessful()) {
               // TODO: What? Log? Pretty spammy...
            }
         }
         catch (final IOException io) {
            // TODO: What? Log? Pretty spammy...
            io.printStackTrace();
         }
      }
   }
}
