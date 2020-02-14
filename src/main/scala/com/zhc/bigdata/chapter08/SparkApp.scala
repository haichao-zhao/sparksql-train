package com.zhc.bigdata.chapter08

import com.zhc.bigdata.chapter08.business.{AppStatProcessor, AreaStatProcessor, LogETLProcessor, ProvinceCityStatProcessor}
import org.apache.spark.sql.SparkSession

object SparkApp {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkApp")
      .getOrCreate()

    // STEP1:ETL
//    LogETLProcessor.process(spark)

    // STEP2:省份地市数据分布统计
//    ProvinceCityStatProcessor.process(spark)

    // STEP3:地域分布情况统计
//    AreaStatProcessor.process(spark)

    // STEP4:App分布情况统计
    AppStatProcessor.process(spark)

    spark.stop()

  }

}
