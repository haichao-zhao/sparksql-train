package com.zhc.bigdata.chapter07

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkKuduApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkKuduApp").master("local").getOrCreate()
    val conf = ConfigFactory.load()
    import spark.implicits._

    //    val driver = conf.getString("db.default.driver")
    //    val url = conf.getString("db.default.url")
    //    val user = conf.getString("db.default.user")
    //    val password = conf.getString("db.default.password")
    //    val database = conf.getString("db.default.database")
    //    val table = conf.getString("db.default.table")
    //
    //    val properties = new Properties()
    //    properties.put("user", user)
    //    properties.put("password", password)
    //    //    properties.put("driver", driver)
    //
    //    val jdbcDF = spark.read
    //      .format("jdbc")
    //      .jdbc(url, s"$database.$table", properties)
    //
    //    jdbcDF.show(truncate = false)

    val KUDU_MASTER = "hadoop000"

    //    jdbcDF.write.mode(SaveMode.Append)
    //      .format("org.apache.kudu.spark.kudu")
    //      .option("kudu.master", KUDU_MASTER)
    //      .option("kudu.table", "pk")
    //      .save()

    spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", "area_stat")
      .load().show()

    spark.stop()

  }

}
