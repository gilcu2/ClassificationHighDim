#!/usr/bin/env bash

CONFIGPATH="."
PROGRAM="../target/scala-2.11/classifier.jar"

spark-submit \
--class com.gilcu2.ExplorationMain \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
$PROGRAM $*
