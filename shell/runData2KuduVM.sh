#!/bin/bash
SPARK_JAR=/home/hadoop/lib/sparksql-train-1.0.jar
JAR_LIB=${SPARK_HOME}/jars
dt="20181007"
${SPARK_HOME}/bin/spark-submit \
--master local \
--name SparkApp \
--conf spark.time=$dt \
--conf spark.raw.path="hdfs://hadoop000:8020/pk/access/$dt" \
--conf spark.ip.path="hdfs://hadoop000:8020/pk/access/ip.txt" \
--conf spark.kudu.master="hadoop000" \
--jars $JAR_LIB/kudu-client-1.7.0.jar,$JAR_LIB/kudu-spark2_2.11-1.7.0.jar,$JAR_LIB/hbase-common-1.1.1.jar \
--class com.zhc.bigdata.chapter08.SparkApp $SPARK_JAR
if [ $? -eq 0 ];then
    echo success
else
    echo fail
    exit 1
fi