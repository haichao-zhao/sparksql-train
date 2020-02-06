package com.zhc.bigdata.chapter04


import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object SqlContextApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SqlContextApp")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)

    val df = sql.read.text("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/input.txt")
    df.show()

    sc.stop()

  }

}
