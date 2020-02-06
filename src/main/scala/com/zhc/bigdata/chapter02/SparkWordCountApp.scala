package com.zhc.bigdata.chapter02

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 词频统计案例
  * 输入：文件
  * 需求：统计出文件中每个单词出现的次数
  * 1）读每一行数据
  * 2）按照分隔符把每一行的数据拆成单词
  * 3）每个单词赋上次数为1
  * 4）按照单词进行分发，然后统计单词出现的次数
  * 5）把结果输出到文件中
  * 输出：文件
  */
object SparkWordCountApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkWordCountApp")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/input.txt")

    rdd.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(ascending = false)
      .map(x => (x._2, x._1))
//      .saveAsTextFile("file:///Users/zhaohaichao/workspace/javaspace/sparksql-train/out")
      .collect().foreach(println)

    sc.stop()
  }
}
