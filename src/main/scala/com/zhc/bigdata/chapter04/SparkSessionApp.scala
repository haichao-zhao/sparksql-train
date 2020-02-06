package com.zhc.bigdata.chapter04

import org.apache.spark.sql.SparkSession

/**
  * 认识SparkSession
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local").getOrCreate()

    val df = spark.read.format("text").load("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/input.txt")
    df.show()

    spark.stop()
  }

}
