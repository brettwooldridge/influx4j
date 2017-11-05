/*
 * Copyright (c) 2017, Brett Wooldridge.  All rights reserved.
 * Copyright (c) 2009, 2013, Oracle and/or its affiliates. All rights reserved.
 * Copyright 2009 Google Inc.  All Rights Reserved.
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

package com.zaxxer.influx4j.util;

/**
 * @author brett.wooldridge at gmail.com
 *
 * References:
 *   http://svn.python.org/projects/python/trunk/Objects/listsort.txt
 *   http://svn.python.org/projects/python/trunk/Objects/listobject.c
 */
public class PrimitiveArraySort {

   public interface IntComparator {
      int compare(int a, int b);
   }

   public static void sort(int[] array, final int len, IntComparator c) {
      if (len < 2) {
         return;
      }

      int initRunLen = countRun(array, len, c);
      binarySort(array, len, initRunLen, c);
   }

   private static void binarySort(final int[] array, final int hi, int start, final IntComparator c) {
      for ( ; start < hi; start++) {
         int pivot = array[start];

         // Set left (and right) to the index where array[start] (pivot) belongs
         int left = 0;
         int right = start;
         /*
          * Invariants:
          *   pivot >= all in [0, left).
          *   pivot <  all in [right, start).
          */
         while (left < right) {
            int mid = (left + right) >>> 1;
            if (c.compare(pivot, array[mid]) < 0)
               right = mid;
            else
               left = mid + 1;
         }

         /*
          * The invariants still hold: pivot >= all in [lo, left) and
          * pivot < all in [left, start), so pivot belongs at left.  Note
          * that if there are elements equal to pivot, left points to the
          * first slot after them -- that's why this sort is stable.
          * Slide elements over to make room for pivot.
          */
         int n = start - left;  // The number of elements to move
         // Switch is just an optimization for arraycopy in default case
         switch (n) {
            case 2:  array[left + 2] = array[left + 1];
            case 1:  array[left + 1] = array[left];
               break;
            default: System.arraycopy(array, left, array, left + 1, n);
         }
         array[left] = pivot;
      }
   }

   @SuppressWarnings("StatementWithEmptyBody")
   private static int countRun(final int[] array, final int hi, final IntComparator c) {
      if (hi == 1) return 1;

      // Locate the end of the run, and if descending reverse the range
      int runHi = 1;
      if (c.compare(array[runHi++], array[0]) < 0) {
         // The range must be descending
         for ( ; runHi < hi && c.compare(array[runHi], array[runHi - 1]) < 0; runHi++);
         reverseRange(array, 0, runHi);
      } else {
         // The range must be ascending
         for ( ; runHi < hi && c.compare(array[runHi], array[runHi - 1]) >= 0; runHi++);
      }

      return runHi;
   }

   /**
    * Reverse the specified range of the specified array.
    *
    * @param array the array to be reversed
    * @param start index of the first element
    * @param end index after the last element
    */
   private static void reverseRange(final int[] array, int start, int end) {
      end--;
      while (start < end) {
         int t = array[start];
         array[start++] = array[end];
         array[end--] = t;
      }
   }
}
