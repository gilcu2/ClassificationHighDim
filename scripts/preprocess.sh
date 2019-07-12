#!/usr/bin/env bash

CONFIGPATH="."
PROGRAM="../target/scala-2.11/classifier.jar"
MAIN=com.gilcu2.PreProcessingMain
OUT=preprocessing.out
ERROUT=preprocessing.errout
if [[ $DEBUG ]];then
    export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005
fi

spark-submit \
--class $MAIN \
--conf "spark.driver.extraClassPath=$CONFIGPATH" \
$PROGRAM $* 2>$ERROUT |tee $OUT

echo Output is in $OUT, error output in $ERROUT
