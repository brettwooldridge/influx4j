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

 /**
 * @author brett.wooldridge at gmail.com
 */
public class InfluxDBFactory {

   public static Builder builder() {
      return new Builder();
   }

   /**
    * Builder for a {@link InfluxDBFactory} instance.  Call {@link InfluxDBFactory#builder()} to
    * create an instance of the {@link Builder}.
    */
   public static class Builder {
      private String retentionPolicy;
      private String database;
      private String username;
      private String password;
      private String host;
      private int port;
      private Protocol protocol;

      public enum Protocol {
         HTTP,
         HTTPS,
         UDP
      }

      private Builder() {
      }

      public Builder setConnection(final String host, final int port, final Protocol protocol) {
         this.host = host;
         this.port = port;
         this.protocol = protocol;
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

      public Builder setDatabase(final String database) {
         this.database = database;
         return this;
      }

      public Builder setRetentionPolicy(final String retentionPolicy) {
         this.retentionPolicy = retentionPolicy;
         return this;
      }

      public InfluxDB build() {
         return new InfluxDB();
      }
   }
}
