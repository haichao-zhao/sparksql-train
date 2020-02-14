package com.zhc.bigdata.chapter08

import com.zhc.bigdata.chapter08.business.{LogETLProcessor, ProvinceCityStatProcessor}
import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkApp")
      .getOrCreate()

    // STEP1:ETL
    LogETLProcessor.process(spark)

    // STEP2:省份地市数据分布统计
    ProvinceCityStatProcessor.process(spark)


    spark.stop()

  }

}
