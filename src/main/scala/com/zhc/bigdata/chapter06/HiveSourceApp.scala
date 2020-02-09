package com.zhc.bigdata.chapter06

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object HiveSourceApp {

  def main(args: Array[String]): Unit = {


    //如果想使用Spark来访问Hive的时候，一定要开启Hive的支持
    val spark: SparkSession = SparkSession.builder().master("local").appName("HiveSourceApp")
      .enableHiveSupport() //切记：一定要加上这个方法
      .getOrCreate()

    import spark.implicits._

    val empDS: Dataset[Row] = spark.table("test_db.emp").filter($"deptno" === 3)

    empDS.show()
    empDS.write.mode("overwrite")
      .format("hive")
      .saveAsTable("test_db.emp2")

    spark.stop()

  }

}
