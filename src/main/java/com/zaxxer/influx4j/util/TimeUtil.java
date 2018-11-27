package com.zaxxer.influx4j.util;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

public class TimeUtil
{
   public static String toTimePrecision(final TimeUnit t) {
      switch (t) {
         case HOURS:
            return "h";
         case MINUTES:
            return "m";
         case SECONDS:
            return "s";
         case MILLISECONDS:
            return "ms";
         case MICROSECONDS:
            return "u";
         case NANOSECONDS:
            return "n";
         default:
            throw new IllegalArgumentException("Time precision must be one of:" + ALLOWED_TIMEUNITS);
      }
   }

   private static final EnumSet<TimeUnit> ALLOWED_TIMEUNITS = EnumSet.of(
      TimeUnit.HOURS,
      TimeUnit.MINUTES,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS,
      TimeUnit.MICROSECONDS,
      TimeUnit.NANOSECONDS);

}
