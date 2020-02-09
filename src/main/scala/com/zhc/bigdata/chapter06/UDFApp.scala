package com.zhc.bigdata.chapter06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDFApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local").appName("UDFApp")
      .getOrCreate()

    import spark.implicits._

    /**
      * 需求：统计每个人爱好的个数
      * pk：3
      * jepson： 2
      *
      *
      * 1）定义函数
      * 2）注册函数
      * 3）使用函数
      */
    val infoRDD: RDD[String] = spark.sparkContext.textFile("file:/Users/zhaohaichao/data/coding385/sparksql-train/data/hobbies.txt")
    val infoDF: DataFrame = infoRDD.map(x => {
      val strings: Array[String] = x.split("###")
      Hobbies(strings(0), strings(1))
    }).toDF()

    //    infoDF.show()

    //TODO 定义函数和注册函数
    spark.udf.register("hobby_num", (s: String) => s.split(",").size)

    infoDF.createOrReplaceTempView("hobbies")

    //自定义函数的使用
    spark.sql("select name,hobbies,hobby_num(hobbies) from hobbies")
      .show(truncate = false)

  }

  case class Hobbies(name: String, hobbies: String)

}
