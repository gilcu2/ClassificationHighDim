#!/usr/bin/env bash

MASTER="local[8]"
CONFIGPATH="."
PROGRAM="../target/scala-2.11/classifier.jar"
MAIN=com.gilcu2.ExplorationMain
OUT=exploration.out
if [[ $DEBUG ]];then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
fi

spark-submit \
--class $MAIN \
--master $MASTER \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
$PROGRAM $* 2>/dev/null |tee $OUT

echo Output is in $OUT, error output in $ERROUT
