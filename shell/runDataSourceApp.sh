#!/bin/bash
SPARK_JAR=/Users/zhaohaichao/workspace/javaspace/sparksql-train/target/sparksql-train-1.0.jar
JAR_LIB=/Users/zhaohaichao/programming/spark-2.4.4-bin-2.6.0-cdh5.7.0/jars
#dt=20190107
/Users/zhaohaichao/programming/spark-2.4.4-bin-2.6.0-cdh5.7.0/bin/spark-submit \
--master yarn \
--name DataSourseApp \
--jars $JAR_LIB/mysql-connector-java-5.1.43.jar \
--class com.zhc.bigdata.chapter05.DataSourseApp $SPARK_JAR
if [ $? -eq 0 ];then
    echo success
else
    echo fail
    exit 1
fi
