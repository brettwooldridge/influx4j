#!/bin/bash

mvn $1 $2 $3 $4 $5 $6 -DskipTests=true -Dmaven.javadoc.skip=true -V -B

./unittest.sh $1 $2 $3 $4 $5 $6
