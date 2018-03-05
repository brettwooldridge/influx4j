#!/bin/bash

BATCH_MODE="-B"
if [[ $1 =~ release ]]; then
   echo "Starting InfluxDB docker for test..."

   docker_id=$(docker run \
               --env INFLUXDB_HTTP_AUTH_ENABLED=true \
               --env INFLUXDB_ADMIN_ENABLED=true \
               --env INFLUXDB_DB=influx4j \
               --env INFLUXDB_USER=influx4j \
               --env INFLUXDB_USER_PASSWORD=influx4j \
               --env INFLUXDB_ADMIN_USER=admin \
               --env INFLUXDB_ADMIN_PASSWORD=password \
               --env INFLUXDB_UDP_ENABLED=true \
               --publish 9086:8086 \
               --publish 9089:8089/udp \
               --tmpfs /var/lib/influxdb:rw,noexec,nosuid,size=65536k \
               --detach influxdb:1.3 influxd)
   docker ps

   BATCH_MODE=""
fi

mvn -DskipTests=true -Dmaven.javadoc.skip=true -V $BATCH_MODE $1 $2 $3 $4 $5 $6

if [[ $1 =~ release ]]; then
   tmp=$(docker rm -f $docker_id)
fi
