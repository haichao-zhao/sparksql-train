package com.zhc.bigdata.chapter04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRDDApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("InteroperatingRDDApp").getOrCreate()
    //    runInfoSchema(spark)


    runProgrammaticSchema(spark)


    spark.stop()
  }

  private def runProgrammaticSchema(spark: SparkSession) = {
    val rdd = spark.sparkContext
      .textFile("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.txt")

    val peopleRowRDD: RDD[Row] = rdd.map(_.split(",")).map(x => Row(x(0), x(1).trim.toLong))

    val struct: StructType = StructType(Array(StructField("name", StringType, true), StructField("age", LongType, true)))

    val peopleDF: DataFrame = spark.createDataFrame(peopleRowRDD, struct)

    peopleDF.show()
  }

  /**
    * 第一种方式：反射
    * 1）定义case class
    * 2）RDD map，map中每一行数据，对应转成case class
    *
    * @param spark
    */
  private def runInfoSchema(spark: SparkSession) = {
    import spark.implicits._

    val rdd = spark.sparkContext
      .textFile("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.txt")
    val peopleDF = rdd
      .map(_.split(","))
      .map(x => Person(x(0), x(1).trim.toLong))
      .toDF()

    //    peopleDF.show()

    peopleDF.createOrReplaceTempView("people")
    val queryDF: DataFrame = spark.sql("select name ,age from people where age between 19 and 29")

    queryDF.map(x => "Name:" + x.getAs[String]("name")).show()
  }

  case class Person(name: String, age: Long)

}
