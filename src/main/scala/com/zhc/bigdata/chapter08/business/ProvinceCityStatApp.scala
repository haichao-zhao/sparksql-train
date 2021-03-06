package com.zhc.bigdata.chapter08.business

import com.zhc.bigdata.chapter08.utils.{KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProvinceCityStatApp {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("ProvinceCityStatApp")
      .getOrCreate()

    import spark.implicits._

    val KUDU_MASTER = "hadoop000"
    val sourceTableName = "ods"
    val toTableName = "province_city_stat"

    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", sourceTableName)
      .load()

//    odsDF.show()

    odsDF.createOrReplaceTempView("ods")

    val sql = SQLUtils.PROVINCE_CITY_SQL
    val res: DataFrame = spark.sql(sql).sort($"cnt".desc)
//    res.show()
    val partitionId = "provincename"

    KuduUtils.sink(res,toTableName,KUDU_MASTER,SchemaUtils.ProvinceCitySchema,partitionId)

    spark.stop()

  }

}
