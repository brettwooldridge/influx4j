#!/bin/bash

echo "Starting InfluxDB docker for test..."

docker_id=$(docker run \
            --env INFLUXDB_UDP_ENABLED=true \
            --publish 9086:8086 \
            --publish 9089:8089/udp \
            --tmpfs /var/lib/influxdb:rw,noexec,nosuid,size=65536k \
            --detach influxdb:1.3-alpine)
sleep 2
docker ps

echo "------------------------------------------------------------------------------"
echo "Creating 'influx4j' database..."
curl -v 'http://localhost:9086/query' --data-urlencode 'q=CREATE DATABASE "influx4j"'

echo "------------------------------------------------------------------------------"

mvn test -B -V

tmp=$(docker rm -f $docker_id)
