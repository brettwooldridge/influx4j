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

import com.zaxxer.influx4j.util.DaemonThreadFactory;
import com.zaxxer.influx4j.util.HexDumpElf;
import com.zaxxer.influx4j.util.TimeUtil;

import okhttp3.*;
import okio.BufferedSink;
import org.jctools.queues.MpscArrayQueue;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDB implements AutoCloseable {

   private static final Logger LOGGER = Logger.getLogger(InfluxDB.class.getName());

   /*****************************************************************************************
    * InfluxDB wire protocols.
    */
   public enum Protocol {
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
   public enum Consistency {
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
   public enum Precision {
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

   public interface InfluxDbListener {
      void outcome(boolean success, long finalSequence);
   }

   public static final int MAXIMUM_SERIALIZED_POINT_SIZE;
   private static final int MAXIMUM_POINT_BATCH_SIZE;
   private static final int SEND_BUFFER_SIZE;
   private static final int HTTP_CONNECT_TIMEOUT;
   private static final int HTTP_READ_TIMEOUT;
   private static final int HTTP_WRITE_TIMEOUT;

   private static final ConcurrentHashMap<URL, SocketConnection> CONNECTIONS = new ConcurrentHashMap<>();

   private final SocketConnection connection;
   private final String baseUrl;
   private final String credentials;

   static {
      HTTP_CONNECT_TIMEOUT = Integer.getInteger("com.zaxxer.influx4j.connect.timeout", 15);
      HTTP_READ_TIMEOUT = Integer.getInteger("com.zaxxer.influx4j.connect.timeout", 15);
      HTTP_WRITE_TIMEOUT = Integer.getInteger("com.zaxxer.influx4j.connect.timeout", 15);

      MAXIMUM_SERIALIZED_POINT_SIZE = Integer.getInteger("com.zaxxer.influx4j.maxSerializedPointSize", 32 * 1024);
      MAXIMUM_POINT_BATCH_SIZE = Integer.getInteger("com.zaxxer.influx4j.maxPointBatchSize", 5000);

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
         SEND_BUFFER_SIZE = Math.max(1024 * 1024, sendBuffSize);
      }
   }

   private InfluxDB(final SocketConnection connection,
                    final String baseUrl,
                    final String credentials) {
      this.connection = connection;
      this.baseUrl = baseUrl;
      this.credentials = credentials;
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
    * Execute a {@link Query}, with the result JSON being returned as a String.
    *
    * @param query the query to execute
    * @return the query result as String
    */
   public String query(final Query query) {
      try {
         try (final StringWriter writer = new StringWriter(1024)) {
            final String q = "db=" + query.getDatabase() + "&q=" + query.getCommandWithUrlEncoded();
            executeQuery(q, writer);

            return writer.toString();
         }
      } catch (final Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Execute a {@link Query}, with the result JSON being returned as a String, with
    * timestamps returned in Unix epoch format with the specific precision.
    *
    * @param query    the query to execute
    * @param timeUnit the time unit precision of the timestamps (in Unix epoch format)
    * @return the query result as String
    */
   public String query(final Query query, TimeUnit timeUnit)
   {
      try
      {
         try (final StringWriter writer = new StringWriter(1024))
         {
            final String q = "db=" + query.getDatabase() + "&q=" + query.getCommandWithUrlEncoded() + "&epoch=" + TimeUtil.toTimePrecision(timeUnit);
            executeQuery(q, writer);

            return writer.toString();
         }
      } catch (final Exception e) {
         throw new RuntimeException(e);
      }
   }

   /**
    * Execute a {@link Query}, with the result JSON being written to the speicifed {@link Writer}.
    *
    * @param query the query to execute
    * @param writer the {@link Writer} into which to write the response
    */
   public void query(final Query query, final Writer writer) {
      try {
         final String q = "db=" + query.getDatabase() + "&q=" + query.getCommandWithUrlEncoded();

         executeQuery(q, writer);
      } catch (final Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Execute a {@link Query}, with the result JSON being written to the speicifed {@link Writer} with
    * timestamps returned in Unix epoch format with the specific precision.
    *
    * @param query    the query to execute
    * @param writer   the {@link Writer} into which to write the response
    * @param timeUnit the time unit precision of the timestamps (in Unix epoch format)
    */
   public void query(final Query query, final Writer writer, final TimeUnit timeUnit)
   {
      try
      {
         final String q = "db=" + query.getDatabase() + "&q=" + query.getCommandWithUrlEncoded() + "&epoch=" + TimeUtil.toTimePrecision(timeUnit);

         executeQuery(q, writer);
      } catch (final Exception e) {
         throw new RuntimeException(e);
      }
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
      try {
         final String query = "db="
              + "&q=create+database+" + URLEncoder.encode(name, "utf8");

         return executeCommand(query);
      }
      catch (final Exception e) {
         throw new RuntimeException(e);
      }
   }

   public String createRetentionPolicy(
      final String name,
      final String dbName,
      final long duration,
      final Precision durationUnit,
      final int replicationFactor,
      final boolean isDefault) {
      return createRetentionPolicy(name, dbName, duration, durationUnit, replicationFactor, 0, durationUnit, isDefault);
   }

   public String createRetentionPolicy(
      final String name,
      final String dbName,
      final long duration,
      final Precision durationUnit,
      final int replicationFactor,
      final long shardDuration,
      final Precision shardDurationUnit,
      final boolean isDefault) {
      try {
         String query = "db="
            + "&q=CREATE+RETENTION+POLICY+%22" + URLEncoder.encode(name, "utf8") + "%22"
            + "+ON+%22" + URLEncoder.encode(dbName, "utf8") + "%22"
            + "+DURATION+" + duration + durationUnit.precision
            + "+REPLICATION+" + replicationFactor;

         if (shardDuration > 0) {
            query += "+SHARD+DURATION+" + shardDuration + shardDurationUnit.precision;
         }
         if (isDefault) {
            query += "+DEFAULT";
         }

         return executeCommand(query);
      }
      catch (final Exception e) {
         throw new RuntimeException(e);
      }
   }


   private static URL createURL(URL baseUrl, String path, final String... queryParameters) {
       try {
          String url = baseUrl.toString() + path;

          if (queryParameters.length > 0) {
             url = url + "?" + String.join("&", queryParameters);
          }

          return new URL(url);
       }
       catch (final Exception e) {
          throw new RuntimeException(e);
       }
    }

   private String executeCommand(final String query) throws IOException, MalformedURLException {
      try {
         final String url = this.baseUrl + "/query?" + query;

         final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(HTTP_CONNECT_TIMEOUT, SECONDS)
            .readTimeout(HTTP_READ_TIMEOUT, SECONDS)
            .writeTimeout(HTTP_WRITE_TIMEOUT, SECONDS)
            .build();

         final Request request = new Request.Builder()
            .url(url)
            .get()
            .addHeader("Authorization", this.credentials)
            .build();

         try (final Response response = client.newCall(request).execute()) {
            return response.body().string();
         }
      } catch (final IOException e) {
         LOGGER.log(Level.SEVERE, "InfluxDB#executeCommand; Unexpected Exception", e);
         throw new RuntimeException(e);
      }
   }

   private void executeQuery(final String query, final Writer writer) {
      try {
         final String url = this.baseUrl + "/query?" + query;

         final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(HTTP_CONNECT_TIMEOUT, SECONDS)
            .readTimeout(HTTP_READ_TIMEOUT, SECONDS)
            .writeTimeout(HTTP_WRITE_TIMEOUT, SECONDS)
            .build();

         final Request request = new Request.Builder()
            .url(url)
            .addHeader("Authorization", this.credentials)
            .build();

         final Response response = client.newCall(request).execute();

         try (final ResponseBody body = response.body();
              final Reader responseStream = response.body().charStream()) {
            int read = 0;
            final char[] cbuf = new char[1024];
            do {
               read = responseStream.read(cbuf);
               if (read > 0) {
                  writer.write(cbuf, 0, read);
               }
            } while (read > 0);
         }
      } catch (final IOException e) {
         LOGGER.log(Level.SEVERE, "InfluxDB#executeQuery; Unexpected Exception", e);
         throw new RuntimeException(e);
      }
   }

   /**
    * Get an instance of an InfluxDB {@link Builder}.
    *
    * @return a Builder for InfluxDB instances.
    */
   public static Builder builder() {
      return new Builder();
   }

   /*****************************************************************************************
    * Builder for an {@link InfluxDB} instance.  Call {@link InfluxDB#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private String retentionPolicy = "autogen";
      private String database;
      private String username;
      private String password;
      private String credentials;
      private long autoFlushPeriod = SECONDS.toNanos(1);
      private URL baseURL;
      private Consistency consistency = Consistency.ONE;
      private Precision precision = Precision.NANOSECOND;
      private ThreadFactory threadFactory;
      private InfluxDbListener listener;

      private Builder() {
      }

      public Builder setConnection(final String host, final int port, final Protocol protocol) {
         try {
         this.baseURL = new URL(protocol.toString(), host, port, "");
      } catch (MalformedURLException e) {
         e.printStackTrace();
      }

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

      public Builder setInfluxDbListener(final InfluxDbListener listener) {
         this.listener = listener;
         return this;
      }

      public InfluxDB build() {
         if (username == null) throw new IllegalStateException("Influx 'username' must be specified.");
         if (password == null) throw new IllegalStateException("Influx 'password' must be specified.");
         if (threadFactory == null) threadFactory = new DaemonThreadFactory("InfluxDB flusher " + baseURL.getHost() + ":" + baseURL.getPort() + "-" + database);

         this.credentials = Credentials.basic(username, password);

         Protocol protocol = Protocol.valueOf(this.baseURL.getProtocol().toUpperCase());
         try {
            SocketConnection connection = null;
            switch (protocol) {
               case HTTP:
               case HTTPS: {
                  if (!validateConnection()) {
                     throw new RuntimeException("Access denied to user '" + username + "'.");
                  }

                  connection = CONNECTIONS.computeIfAbsent(
                     InfluxDB.createURL(this.baseURL,
                                 "/write",
                                 "db=" + database,
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

            return new InfluxDB(connection, this.baseURL.toString(), credentials);
         }
         catch (final IOException e) {
            throw new RuntimeException(e);
         }
      }

      private SocketConnection createConnection(final URL url) {
         try {
            return new SocketConnection(url, credentials, precision, autoFlushPeriod, threadFactory, listener);
         }
         catch (final Exception e) {
            throw new RuntimeException(e);
         }
      }

      boolean validateConnection() throws IOException {
         final URL url = InfluxDB.createURL(this.baseURL, "/query", "q=" + URLEncoder.encode("SHOW DATABASES", "utf8"));

         final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(HTTP_CONNECT_TIMEOUT, SECONDS)
            .readTimeout(HTTP_READ_TIMEOUT, SECONDS)
            .writeTimeout(HTTP_WRITE_TIMEOUT, SECONDS)
            .build();

         final Request request = new Request.Builder()
            .url(url.toString())
            .addHeader("Authorization", this.credentials)
            .build();

         final Call call = client.newCall(request);
         try (final Response response = call.execute()) {
            final int status = response.code();
            if (status < 300) {
               return true;
            }
            else if (status == 401) {
               return false;
            }
            throw new IOException("Unexpected response code (" + status + ") during connection validation");
         }
      }
   }


   /*****************************************************************************************
    * SocketConnection is used for HTTP/S protocol interactions.
    */
   private static class SocketConnection implements Runnable {
      private static final MediaType MEDIA_TYPE_TEXT = MediaType.parse("text/plain; charset=utf-8");
      private static final int QUEUE_SIZE = 64 * 1024;
      private static final int QUEUE_RETRY_LIMIT = 48 * 1024;

      private final OkHttpClient client;
      private final Semaphore shutdownSemaphore;
      private final Precision precision;
      private final MpscArrayQueue<Point> pointQueue;
      private final URL url;
      private final String credentials;
      private final long autoFlushPeriod;
      private volatile boolean shutdown;
      private final InfluxDbListener listener;

      SocketConnection(final URL url,
                       final String credentials,
                       final Precision precision,
                       final long autoFlushPeriod,
                       final ThreadFactory threadFactory,
                       final InfluxDbListener listener) throws IOException {
         this.url = url;
         this.credentials = credentials;
         this.precision = precision;
         this.autoFlushPeriod = autoFlushPeriod;
         this.listener = listener;
         this.pointQueue = new MpscArrayQueue<>(QUEUE_SIZE);
         this.shutdownSemaphore = new Semaphore(1);
         this.shutdownSemaphore.acquireUninterruptibly();
         this.client = new OkHttpClient.Builder()
            .retryOnConnectionFailure(true)
            .connectTimeout(HTTP_CONNECT_TIMEOUT, SECONDS)
            .readTimeout(HTTP_READ_TIMEOUT, SECONDS)
            .writeTimeout(HTTP_WRITE_TIMEOUT, SECONDS)
            .cookieJar(new CookieJar() {
               private List<Cookie> cookies;

               @Override
               public void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
                  this.cookies =  cookies;
               }

               @Override
               public List<Cookie> loadForRequest(HttpUrl url) {
                  if (cookies != null)
                     return cookies;
                  return new ArrayList<>();
               }
            })
            .build();

         final Thread flusher = threadFactory.newThread(this);
         flusher.setDaemon(true);
         flusher.start();
      }

      void write(final Point point) {
         point.check();

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

         class InfluxRequestBody extends RequestBody {
            private int length;

            @Override public long contentLength() {
               if (buffer.position() > 0) {
                  buffer.flip();
                  buffer.mark();
                  length = buffer.remaining();
               }
               return length;
            }

            @Override public MediaType contentType() {
               return MEDIA_TYPE_TEXT;
             }

            @Override public void writeTo(BufferedSink sink) throws IOException {
               sink.write(buffer.array(), 0, length);
               sink.flush();
            }
         }

         final InfluxRequestBody requestBody = new InfluxRequestBody();

         final Request request = new Request.Builder()
            .url(url)
            .post(requestBody)
            .addHeader("Authorization", credentials)
            .build();

         final Call httpCall = client.newCall(request);

         final Supplier<Boolean> writeBuffers = () -> {
            boolean succeeded = true;
            do {
               final Call call = httpCall.clone();
               try (Response response = call.execute()) {
                  if (response.isSuccessful()) break;

                  final String message = response.message();
                  LOGGER.severe("Error persisting points.  Response code: " + response.code()
                                 + ", message " + message
                                 + ".  Response body:\n" + response.body().string());

                  if (!message.contains("timeout")) {
                     LOGGER.severe("Insertion failed with a non-recoverable error, dropping point batch.");
                     succeeded = false;
                     break;
                  }
               }
               catch (final IOException io) {
                  LOGGER.log(Level.SEVERE, "Exception persisting points.  Message: " + io.getLocalizedMessage(), io);
               }

               if (LOGGER.isLoggable(Level.FINEST)) {
                  LOGGER.finest("Request buffer: \n" + HexDumpElf.dump(0, buffer.array(), 0, requestBody.length));
               }

               if (pointQueue.size() > QUEUE_RETRY_LIMIT) {
                  LOGGER.severe("Retry has not succeeded and the pending queue has exceeded 75% capacity, dropping point batch.");
                  succeeded = false;
                  break;
               }

               buffer.reset();
               LockSupport.parkNanos(autoFlushPeriod);
            } while (true);

            buffer.clear();
            return succeeded;
         };

         try {
            while (!shutdown) {
               final long startNs = nanoTime();
               final boolean debug = LOGGER.isLoggable(Level.FINE);

               int batchSize = 0;
               long lastPointSequence = 0;
               do {
                  try (final Point point = pointQueue.poll()) {
                     if (point == null) break;

                     if (debug && batchSize == 0) LOGGER.fine("First point in batch " + point);
                     point.write(buffer, precision);
                     lastPointSequence = point.getSequence();
                  }
               } while (buffer.remaining() >= MAXIMUM_SERIALIZED_POINT_SIZE && ++batchSize < MAXIMUM_POINT_BATCH_SIZE);

               if (buffer.position() > 0) {
                  final boolean again = buffer.remaining() < MAXIMUM_SERIALIZED_POINT_SIZE;

                  final boolean success = writeBuffers.get();
                  if (listener != null) {
                     listener.outcome(success, lastPointSequence);
                  }

                  if (debug) LOGGER.fine("InfluxDB HTTP write time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs) + "ms");

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
   }
}
