package com.zhc.bigdata.chapter04

import org.apache.spark.sql.SparkSession

object DataFrameAPIApp {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local").appName("DataFrameAPIApp").getOrCreate()

    //    val df = spark.read.json("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/people.json")

    import spark.implicits._

    // TODO ... DF里面有两列，只要name列 ==> select name from people
    //    df.select("name").show()
    //    df.select($"name").show()

    // TODO ... select * from people where age > 21
    //    df.filter(df("age") > 21).show()
    //    df.filter($"age" > 21).show()
    //    df.filter("age > 21").show()

    // TODO ... select age,count(1) from people group by age
    //    df.groupBy("age").count().show()

    // TODO ... 使用SQL方式操作
    // 创建临时表
    //    df.createOrReplaceTempView("people")
    //    spark.sql("select * from people where age > 21").show()

    val zips = spark.read.json("file:///Users/zhaohaichao/data/coding385/sparksql-train/data/zips.json")

    //    zips.printSchema()
    //    zips.show(truncate = false,numRows = 10)
    //    zips.take(10).foreach(println)

    //    println(zips.count())

    // 过滤出pop大于40000的数据,列重命名
    //    zips.filter("pop > 40000").withColumnRenamed("_id","id").show()

    import org.apache.spark.sql.functions._
    //    zips.select($"_id".as("id"), $"city", $"pop", $"state")
    //      .filter(zips("state") === "CA")
    //      .orderBy(desc("pop"))
    //      .show(10)
    zips.createOrReplaceTempView("zips")

    spark.sql("select _id as id,city,pop,state from zips where state = 'CA' order by pop desc limit 10 ").show()

    spark.stop()

  }

}
