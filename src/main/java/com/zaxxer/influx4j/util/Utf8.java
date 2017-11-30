/*
 * Copyright (c) 2013 The Guava Authors
 * Copyright (c) 2017 Brett Woolridge.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.zaxxer.influx4j.util;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;

public class Utf8 {
   public static boolean containsUnicode(final String string) {
      for (int i = 0; i < string.length(); i++) {
         if (string.charAt(i) > 0x7f) {
            return true;
         }
      }

      return false;
   }

   /**
    * Returns the number of bytes in the UTF-8-encoded form of {@code sequence}. For a string, this
    * method is equivalent to {@code string.getBytes(UTF_8).length}, but is more efficient in both
    * time and space.
    *
    * @param sequence the CharSequence to measure
    * @return the length in bytes of the UTF-8 representation of the CharSequence
    * @throws IllegalArgumentException if {@code sequence} contains ill-formed UTF-16 (unpaired
    *     surrogates)
    */
   public static int encodedLength(final CharSequence sequence) {
      // Warning to maintainers: this implementation is highly optimized.
      int utf16Length = sequence.length();
      int utf8Length = utf16Length;
      int i = 0;

      // This loop optimizes for pure ASCII.
      while (i < utf16Length && sequence.charAt(i) < 0x80) {
         i++;
      }

      // This loop optimizes for chars less than 0x800.
      for (; i < utf16Length; i++) {
         char c = sequence.charAt(i);
         if (c < 0x800) {
            utf8Length += ((0x7f - c) >>> 31); // branch free!
         } else {
            utf8Length += encodedLengthGeneral(sequence, i);
            break;
         }
      }

      if (utf8Length < utf16Length) {
         // Necessary and sufficient condition for overflow because of maximum 3x expansion
         throw new IllegalArgumentException(
                 "UTF-8 length does not fit in int: " + (utf8Length + (1L << 32)));
      }
      return utf8Length;
   }

   private static int encodedLengthGeneral(CharSequence sequence, int start) {
      int utf16Length = sequence.length();
      int utf8Length = 0;
      for (int i = start; i < utf16Length; i++) {
         char c = sequence.charAt(i);
         if (c < 0x800) {
            utf8Length += (0x7f - c) >>> 31; // branch free!
         } else {
            utf8Length += 2;
            // jdk7+: if (Character.isSurrogate(c)) {
            if (MIN_SURROGATE <= c && c <= MAX_SURROGATE) {
               // Check that we have a well-formed surrogate pair.
               if (Character.codePointAt(sequence, i) == c) {
                  throw new IllegalArgumentException("Unpaired surrogate pair at index " +i);
               }
               i++;
            }
         }
      }
      return utf8Length;
   }
}
