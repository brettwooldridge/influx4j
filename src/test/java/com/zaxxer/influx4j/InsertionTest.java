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

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.zaxxer.influx4j.util.DaemonThreadFactory;


public class InsertionTest {
   private PointFactory pointFactory;
   private InfluxDB influxDB;

   private String host = "127.0.0.1";
   private int port = 9086;
   private InfluxDB.Protocol protocol = InfluxDB.Protocol.HTTP;
   private String username = "influx4j";
   private String password = "influx4j";
   private String database = "influx4j";


   @Before
   public void createFactory() throws Exception {
      pointFactory = PointFactory.builder().build();

      influxDB = InfluxDB.builder()
         .setConnection(host, port, protocol)
         .setUsername(username)
         .setPassword(password)
         .setDatabase(database)
         .setThreadFactory(new DaemonThreadFactory("Flusher"))
         .build();
   }

   @After
   public void shutdownFactory() throws Exception {
      influxDB.close();
   }

   @Test(expected = IllegalStateException.class)
   public void testNoField() {
      final Point point = pointFactory.createPoint("testMeasurement");
      influxDB.write(point);
   }

   @Test
   public void testSingleInsert() throws Exception {
      final Point point = pointFactory.createPoint("testSingleInsert")
         .tag("fruit", "apple")
         .field("yummy", true)
         .timestamp(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

      influxDB.write(point);

      TimeUnit.SECONDS.sleep(1); // to allow async flush to run before querying


      final Query query = Query.builder()
         .setCommand("SELECT fruit, yummy FROM testSingleInsert")
         .setDatabase(database)
         .build();

      String result = influxDB.query(query);
      Assert.assertNotNull(result);
      Assert.assertTrue(result.startsWith("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"testSingleInsert\",\"columns\":[\"time\",\"fruit\",\"yummy\"],\"values\":"));
   }

   @Test
   public void testMultipleInserts() throws Exception {
      final long timeNs = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
      for (int i = 0; i < 1000; i++) {
         final Point point = pointFactory.createPoint("testMultipleInserts")
            .tag("fruit", "banana")
            .field("count", i)
            .timestamp(timeNs + i, TimeUnit.NANOSECONDS);

         influxDB.write(point);
      }

      TimeUnit.SECONDS.sleep(1); // to allow async flush to run before querying

      final Query query = Query.builder()
         .setCommand("SELECT fruit, count FROM testMultipleInserts")
         .setDatabase(database)
         .build();

      String result = influxDB.query(query);
      Assert.assertNotNull(result);
      Assert.assertTrue(result.startsWith("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"testMultipleInserts\",\"columns\":[\"time\",\"fruit\",\"count\"],\"values\":"));
   }

   @Test
   public void testBackSlashAsLastFieldValueMultipleInserts() throws Exception {
      final long timeNs = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());

      final String[] paths = new String [] {
         "A:\\",
         "path with space",   // Spaces being present only changes the error from " unbalanced quotes" to "bad timestamp"
      };

      IntStream.range(0, paths.length)
         .forEach(i -> {
               final Point point = pointFactory.createPoint("testEscapeLastCharacterInserts")
                  .tag("fruit", "banana" + i)
                  .field("path", paths[i])
                  .timestamp(timeNs, TimeUnit.NANOSECONDS);
               influxDB.write(point);
            }
         );

      TimeUnit.SECONDS.sleep(1); // to allow async flush to run before querying

      final Query query = Query.builder()
         .setCommand("SELECT COUNT(\"path\") FROM testEscapeLastCharacterInserts")
         .setDatabase(database)
         .build();

      final String result = influxDB.query(query).trim();
      Assert.assertTrue(result.startsWith("{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"testEscapeLastCharacterInserts\",\"columns\":[\"time\",\"count\"],\"values\":"));
      Assert.assertTrue(result.endsWith("Z\",2]]}]}]}"));
   }
}
