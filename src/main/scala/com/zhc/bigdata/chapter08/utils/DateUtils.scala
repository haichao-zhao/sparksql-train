package com.zhc.bigdata.chapter08.utils

import org.apache.spark.sql.SparkSession

object DateUtils {
  def getTableName(tableName: String, spark: SparkSession) = {
    val time = spark.sparkContext.getConf.get("spark.time") // spark框架只认以spark.开头的参数，否则系统不识别
    tableName + "_" + time
  }
}
