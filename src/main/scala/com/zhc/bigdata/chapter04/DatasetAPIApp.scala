package com.zhc.bigdata.chapter04

import org.apache.spark.sql.{Dataset, SparkSession}

object DatasetAPIApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("DatasetAPIApp").getOrCreate()
    import spark.implicits._


    //创建Dataset
    val ds: Dataset[Person] = Seq(Person("pk", 30)).toDS()
    val primitiveDS = Seq(1, 2, 3).toDS()

    val df = spark.read.json("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.json")

    val peopleDS: Dataset[Person] = df.as[Person]
    peopleDS.show()

    primitiveDS.map(_ + 1).collect().foreach(println)

//    ds.show()

    spark.stop()
  }

  case class Person(name: String, age: Long)

}
