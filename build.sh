#!/bin/bash

mvn -DskipTests=true -Dmaven.javadoc.skip=true -V -B $1 $2 $3 $4 $5 $6
