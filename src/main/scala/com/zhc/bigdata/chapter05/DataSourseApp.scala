package com.zhc.bigdata.chapter05

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataSourseApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSourseApp").getOrCreate()

    //    text(spark)

    //    json(spark)

    //    common(spark)

    //    parquet(spark)

    //    json2parquet(spark)

    //    val jdbcDF = spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306/imoocbootscala?useSSL=false")
    //      .option("dbtable", "meta_database")
    //      .option("user", "root")
    //      .option("password", "root")
    //      .load()


    jdbc(spark)

    spark.stop()
  }

  //读写 jdbc 操作MySQL
  private def jdbc(spark: SparkSession) = {
    val conf = ConfigFactory.load()

    val driver = conf.getString("db.default.driver")
    val url = conf.getString("db.default.url")
    val user = conf.getString("db.default.user")
    val password = conf.getString("db.default.password")
    val database = conf.getString("db.default.database")
    val table = conf.getString("db.default.table")

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    //    properties.put("driver", driver)

    val jdbcDF = spark.read
      .format("jdbc")
      .jdbc(url, s"$database.$table", properties)

    jdbcDF.show(truncate = false)

    jdbcDF.select("location", "name")
      .write.mode("append")
      .jdbc(url, s"$database.$table", properties)
  }

  //读取json文件，写出parquet文件
  private def json2parquet(spark: SparkSession) = {
    import spark.implicits._
    val json = spark.read.format("json").load("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.json")

    json.show()
    json.filter("age > 20").select($"name").write.mode("overwrite").save("file:///Users/zhaohaichao/data/out/json2parquet")

    val parquetDF = spark.read.parquet("file:///Users/zhaohaichao/data/out/json2parquet")
    parquetDF.show()
  }

  // 读写parquet 文件
  private def parquet(spark: SparkSession) = {
    import spark.implicits._

    val parquetDF = spark.read.parquet("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/users.parquet")
    parquetDF.show()

    parquetDF.select($"name").write.mode("overwrite").save("file:///Users/zhaohaichao/data/out/parquet")

    val parquetDF1 = spark.read.parquet("file:///Users/zhaohaichao/data/out/parquet")
    parquetDF1.show()
  }

  //Data Source API 标准写法
  private def common(spark: SparkSession) = {
    val txt = spark.read.format("text").load("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.txt")
    val json = spark.read.format("json").load("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.json")

    txt.show()
    json.show()

    json.write.format("json").mode("overwrite").save("file:///Users/zhaohaichao/data/out/json")
    txt.write.format("text").mode("overwrite").save("file:///Users/zhaohaichao/data/out/text")
  }

  // 读写json 文件
  private def json(spark: SparkSession) = {
    import spark.implicits._

    val jsonDF1 = spark.read.json("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.json")

    //只要年龄大于20的结果
    //    jsonDF1.filter("age > 20").write.mode("overwrite").json("file:///Users/zhaohaichao/data/out/json")


    val jsonDF2 = spark.read.json("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people2.json")
    val frame = jsonDF2.select($"name", $"age", $"info.work".as("work"), $"info.home".as("home"))
    frame.write.mode("overwrite").json("file:///Users/zhaohaichao/data/out/json")

  }

  // 读写text 文件
  private def text(spark: SparkSession) = {
    import spark.implicits._

    val textDF = spark.read.text("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.txt")

    //    textDF.show()

    val textDS: Dataset[(String, Int)] = textDF.map(x => {
      val strings = x.getString(0).split(",")
      (strings(0).trim, strings(1).trim.toInt)
    })

    textDS.withColumnRenamed("_1", "name").withColumnRenamed("_2", "age").show()

    val df: DataFrame = textDS.select($"_1".as("Name"), $"_2".as("age"))

    //    textDS.write.save("file:///Users/zhaohaichao/data/out")
    val value: Dataset[String] = df.map(x => (x(0) + "|" + x(1).toString))
    value.write.mode(SaveMode.Overwrite).text("file:///Users/zhaohaichao/data/out/text")

  }
}
