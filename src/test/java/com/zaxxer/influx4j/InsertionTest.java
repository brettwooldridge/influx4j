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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import com.zaxxer.influx4j.util.DaemonThreadFactory;


public class InsertionTest {
   private PointFactory pointFactory;
   private InfluxDB influxDB;

   @Before
   public void createFactory() throws Exception {
      pointFactory = PointFactory.builder().build();

      influxDB = InfluxDB.builder()
         .setConnection("127.0.0.1", 9086, InfluxDB.Protocol.HTTP)
         .setUsername("influx4j")
         .setPassword("influx4j")
         .setDatabase("influx4j")
         .setThreadFactory(new DaemonThreadFactory("Flusher"))
         .build();
   }

   @After
   public void shutdownFactory() throws Exception {
      influxDB.close();
   }

   @Test
   public void testSingleInsert() throws Exception {
      final Point point = pointFactory.createPoint("testSingleInsert")
         .tag("fruit", "apple")
         .field("yummy", true)
         .timestamp(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

      influxDB.write(point);

      TimeUnit.SECONDS.sleep(1); // to allow async flush to run before querying

      // TODO query and verify
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

      // TODO query and verify
   }
}
