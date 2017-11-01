#!/bin/bash

JAVA_OPTIONS="-server"

# Memory
JAVA_OPTIONS="$JAVA_OPTIONS -Xms750m"
JAVA_OPTIONS="$JAVA_OPTIONS -Xmx1250m"
JAVA_OPTIONS="$JAVA_OPTIONS -Xss256k"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:MetaspaceSize=192m"

# G1GC
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseG1GC"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:MaxGCPauseMillis=100"
#JAVA_OPTIONS="$JAVA_OPTIONS -XX:+ParallelRefProcEnabled"
#JAVA_OPTIONS="$JAVA_OPTIONS -XX:+AlwaysPreTouch"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+ExplicitGCInvokesConcurrent"

# Optimizations
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+OptimizeStringConcat"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseStringDeduplication"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseFastAccessorMethods"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseCompressedClassPointers"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseCompressedOops"
JAVA_OPTIONS="$JAVA_OPTIONS -XX:+AggressiveOpts"

if [[ "clean" == "$1" ]]; then
   mvn clean package
   shift
fi

JMH_THREADS="-t 8"
if [[ "$2" == "-t" ]]; then
   JMH_THREADS="-t $3"
   set -- "$1" "${@:4}"
fi

case "$1" in
   quick)
      java -jar ./target/microbenchmarks.jar -jvmArgs "$JAVA_OPTIONS" -wi 3 -i 8 $JMH_THREADS -f 2 $2 $3 $4 $5 $6 $7 $8 $9
      ;;
   medium)
      java -jar ./target/microbenchmarks.jar -jvmArgs "$JAVA_OPTIONS" -wi 3 -f 8 -i 6 $JMH_THREADS $2 $3 $4 $5 $6 $7 $8 $9
      ;;
   long)
      java -jar ./target/microbenchmarks.jar -jvmArgs "$JAVA_OPTIONS" -wi 3 -i 15 $JMH_THREADS $2 $3 $4 $5 $6 $7 $8 $9
      ;;
   debug)
      java -server $JAVA_OPTIONS -Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=y -jar ./target/microbenchmarks.jar -r 5 -wi 3 -i 8 -t 8 -f 0 $2 $3 $4 $5 $6 $7 $8 $9
      ;;
   gcprof)
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+PrintGCDateStamps"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+PrintGCDetails"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+PrintReferenceGC"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+PrintAdaptiveSizePolicy"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+PrintTenuringDistribution"
      JAVA_OPTIONS="$JAVA_OPTIONS -Xloggc:g1gc.log"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:+UseGCLogFileRotation"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:NumberOfGCLogFiles=5"
      JAVA_OPTIONS="$JAVA_OPTIONS -XX:GCLogFileSize=10M"
      java -jar ./target/microbenchmarks.jar -jvmArgs "$JAVA_OPTIONS" -wi 4 -i 50 $JMH_THREADS -f 1 $2 $3 $4 $5 $6 $7 $8 $9
      ;;
   *)
      java -jar ./target/microbenchmarks.jar -jvmArgs "$JAVA_OPTIONS" -wi 3 -i 15 -t 8 $1 $2 $3 $4 $5 $6 $7 $8 $9
      ;;
esac
