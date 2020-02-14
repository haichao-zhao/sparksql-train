package com.zhc.bigdata.chapter08.business

import com.zhc.bigdata.chapter08.utils.{IPUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.kudu.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 日志ETL清洗操作
  */
object LogETLApp {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("LogETLApp")
      .getOrCreate()

    //    val jsonDF: DataFrame = spark.read
    //      .json("hdfs://localhost:8020/data/data-test.json")

    //读取原始数据日志
    var jsonDF: DataFrame = spark.read
      .json("file:///Users/zhaohaichao/workspace/javaspace/sparksql-train/data/data-test.json")

    //    jsonDF.printSchema()
    //    jsonDF.show()

    //读取ip规则库
    val ipRowRDD: RDD[String] = spark.sparkContext
      .textFile("file:///Users/zhaohaichao/workspace/javaspace/sparksql-train/data/ip.txt")


    import spark.implicits._

    //取出想用的字段
    val ipRuleDF: DataFrame = ipRowRDD.map(x => {
      val splits: Array[String] = x.split("\\|")
      val startIP: Long = splits(2).toLong
      val endIP: Long = splits(3).toLong
      val province: String = splits(6)
      val city: String = splits(7)
      val isp: String = splits(9)
      (startIP, endIP, province, city, isp)
    }).toDF("start_ip", "end_ip", "province", "city", "isp")

    //    ipRuleDF.show(false)
    // json中的ip转换一下， 通过Spark SQL UDF函数
    import org.apache.spark.sql.functions._
    def getIpLog() = udf((ip: String) => {
      IPUtils.ip2Long(ip)
    })

    jsonDF = jsonDF.withColumn("ip_long", getIpLog()($"ip"))

    // 两个DF进行join，条件是jsonDF中的ip_long 是在规则ip中的范围内，ip_long between ... and ...
    //    jsonDF.join(ipRuleDF, jsonDF("ip_long")
    //      .between(ipRuleDF("start_ip"), ipRuleDF("end_ip")))
    //      .show()


    jsonDF.createOrReplaceTempView("logs")
    ipRuleDF.createOrReplaceTempView("ips")

    val sql = SQLUtils.SQL

    val res: DataFrame = spark.sql(sql)

    // ETL处理之后需要落地到Kudu

    //    val KUDU_MASTER = "tencent"
    val KUDU_MASTER = "hadoop000"

    val tableName = "ods"
    val schema: Schema = SchemaUtils.ODSSchema
    val partitionId = "ip"
    KuduUtils.sink(res, tableName, KUDU_MASTER, schema, partitionId)

    spark.stop()
  }
}
