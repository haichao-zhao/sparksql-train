#!/bin/bash
SPARK_JAR=/workspace/data/sparksql-train-1.0.jar
JAR_LIB=${SPARK_HOME}/jars
dt=20181007
${SPARK_HOME}/bin/spark-submit \
--master local \
--name SparkApp \
--conf spark.time=$dt \
--conf spark.raw.path="hdfs://localhost:8020/pk/access/$dt" \
--conf spark.ip.path="hdfs://localhost:8020/pk/access/ip.txt" \
--conf spark.kudu.master="localhost" \
--jars $JAR_LIB/mysql-connector-java-5.1.43.jar \
--class com.zhc.bigdata.chapter08.SparkApp $SPARK_JAR
if [ $? -eq 0 ];then
    echo success
else
    echo fail
    exit 1
fi