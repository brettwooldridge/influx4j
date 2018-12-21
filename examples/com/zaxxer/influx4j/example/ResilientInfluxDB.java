package com.zaxxer.influx4j.example;

import com.zaxxer.influx4j.InfluxDB;
import com.zaxxer.influx4j.InfluxDB.InfluxDbListener;
import com.zaxxer.influx4j.Point;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResilientInfluxDB implements InfluxDbListener {

   private final InfluxDB influxDB;
   private final Queue<Point> pendingQueue = new LinkedBlockingDeque<>();

   public ResilientInfluxDB(final InfluxDB influxDB) {
      this.influxDB = influxDB;
   }

   public void write(final Point point) {
      // Prevent the point from being returned to the pool by the write() operation, so we still have access
      // to the point in the outcome() callback below.
      point.retain();

      // Add the point to our pending queue to be released in the outcome() callback below.
      pendingQueue.add(point);

      // Forward the write() call to the influx4j driver.
      influxDB.write(point);
   }

   public void recover() {
      // Implementation of this method is an exercise left to the reader (you).  Basically, you probably want
      // to iterate the "backlog" directory contents, and process (in order) any "backlog-xxxxxxx" files that
      // exist there, removing them as you go.

      // The contents of the files are directly "POST"-able over HTTP/S to InfluxDB as their contents are in
      // valid InfluxDB wire protocol format.
   }

   /* **************************************************************************
    *  InfluxDbListener interface methods
    */

   /***************************************************************************
    *
    * @param success true if the batch insert was successful, false otherwise
    * @param finalSequence the sequence number of the last Point in the batch
    */
   @Override
   public void outcome(boolean success, long finalSequence) {
      try {
         if (success) return;

         // If we got here, InfluxDB gave up on the batch of points ending with the Point having the
         // finalSequence number passed into this method. iterate over the points up to this sequence
         // number and serialize them out to a file...

         final Path backlogFile = Paths.get("backlog", "backlog-" + finalSequence);
         try (Writer writer = new BufferedWriter(new FileWriter(backlogFile.toFile()))) {
            while (!pendingQueue.isEmpty() && pendingQueue.peek().getSequence() <= finalSequence) {
               final Point point = pendingQueue.poll();
               assert point != null;
               writer.write(point.toString());
               writer.write('\n');
               point.close();
            }
         } catch (IOException e) {
            Logger.getGlobal().log(Level.WARNING, "Could not serialize to backlog file", e);
         }
      }
      finally {
         // Iterate over points in our pendingQueue and "close" them so they can be recycled by the pool.
         while (!pendingQueue.isEmpty() && pendingQueue.peek().getSequence() <= finalSequence) {
            final Point point = pendingQueue.poll();
            assert point != null;
            point.close();
         }
      }
   }
}
