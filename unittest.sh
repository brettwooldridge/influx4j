#!/bin/bash

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

#echo "------------------------------------------------------------------------------"
#echo "Creating 'influx4j' database..."
#curl -v 'http://localhost:9086/query' --data-urlencode 'q=CREATE DATABASE "influx4j"'

echo "------------------------------------------------------------------------------"

debug="-DforkCount=1"
if [[ "$1" = "debug" ]]; then
   debug="-Dmaven.surefire.debug -DforkCount=0"
elif [[ "$1" = "profile" ]]; then
   debug="-agentpath:/Applications/JProfiler.app/Contents/Resources/app/bin/macos/libjprofilerti.jnilib=port=8849 -DforkCount=0"
fi

mvn $debug test -B -V

tmp=$(docker rm -f $docker_id)
