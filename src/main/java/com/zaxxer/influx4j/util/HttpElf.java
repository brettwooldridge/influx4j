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

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class HttpElf {
   private HttpElf() {
      // unconstructable
   }

   public static String readResponse(final SocketChannel socketChannel, final ByteBuffer buffer) throws IOException {
      final byte[] bytes = buffer.array();
      final long startNs = System.nanoTime();

      // SocketChannel does not support read timeouts
      final Socket rawSocket = socketChannel.socket();

      try {
         rawSocket.setSoTimeout((int) SECONDS.toMillis(5));
         do {
            final int read = socketChannel.read(buffer);
            if (read < 0) {
               break;
            }
            else if (buffer.position() > 4) {
               // scan buffer back to front looking for the CRLF+CRLF that terminates an HTTP header block
               for (int offset = buffer.position() - 1; offset > 4; offset--) {
                  if (bytes[offset] == '\n' && offset > 4 && bytes[offset - 1] == '\r' &&bytes[offset - 2] == '\n' &&bytes[offset - 3] == '\r') {
                     final String response = new String(bytes, 0, buffer.position());
                     if (response.contains("Content-Length:")) {
                        consumeRemaining(socketChannel, buffer, response, offset + 4);
                     }
                     return response;
                  }
               }
            }
            else {
               LockSupport.parkNanos(10000L);
            }
         } while (rawSocket.isConnected());

         throw new IOException("Unexpected end-of-stream");
      }
      finally {
         rawSocket.setSoTimeout(0);
         buffer.clear();
         System.out.println("Time in read " + NANOSECONDS.toMicros(System.nanoTime() - startNs) + "us");
      }
   }

   private static void consumeRemaining(final SocketChannel socketChannel, final ByteBuffer buffer, final String response, final int offset) throws IOException {
      Pattern pattern = Pattern.compile("Content-Length: (\\d+)");
      Matcher matcher = pattern.matcher(response);
      if (matcher.find()) {
         final int length = Integer.valueOf(matcher.group(1).toString());
         final int remainder = length - (buffer.position() - offset);
         int consumed = 0;
         while (consumed < remainder) {
            final int read = socketChannel.read(buffer);
            if (read < 0) {
               throw new IOException("Unexpected end-of-stream");
            }
            else if (read > 0) {
               consumed += read;
            }
            else {
               LockSupport.parkNanos(10000L);
            }
         }
      }
   }
}
