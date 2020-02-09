package com.zhc.bigdata.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BuildinFunctionApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("BuildinFunctionApp")
      .getOrCreate()

    import spark.implicits._

    // 工作中，源码托管在gitlab，svn，cvs，你clone下来以后，千万不要手贱，做代码样式的格式化
    val userAccessLog = Array(
      "2016-10-01,1122", // day  userid
      "2016-10-01,1122",
      "2016-10-01,1123",
      "2016-10-01,1124",
      "2016-10-01,1124",
      "2016-10-02,1122",
      "2016-10-02,1121",
      "2016-10-02,1123",
      "2016-10-02,1123"
    )

    val userAccessRDD: RDD[String] = spark.sparkContext.parallelize(userAccessLog)

    val userAccessDF: DataFrame = userAccessRDD.map(x => {
      val strings: Array[String] = x.split(",")
      Log(strings(0).trim, strings(1).trim.toInt)
    }).toDF()

    import org.apache.spark.sql.functions._

//    userAccessDF.show()
    userAccessDF.groupBy("day")
      .agg(count("userId").as("pv"))
      .show()

    userAccessDF.groupBy("day")
      .agg(countDistinct("userId").as("uv"))
      .show()

    spark.stop()
  }

  case class Log(day: String, userId: Int)

}
