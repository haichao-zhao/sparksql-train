package com.zhc.bigdata.chapter08.business

import com.zhc.bigdata.chapter08.`trait`.DataProcess
import com.zhc.bigdata.chapter08.utils._
import org.apache.kudu.Schema
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 广播变量方式实现join
  */
object LogETLBroadcastProcessor extends DataProcess {
  override def process(spark: SparkSession): Unit = {

    import spark.implicits._

    //读取原始数据日志
    var jsonDF: DataFrame = spark.read.json("file:///Users/zhaohaichao/workspace/javaspace/sparksql-train/data/data-test.json")
    //    val rawPath: String = spark.sparkContext.getConf.get("spark.raw.path")
    //    var jsonDF: DataFrame = spark.read.json(rawPath)

    //    jsonDF.printSchema()
    //    jsonDF.show()

    //读取ip规则库
    //    val ipPath: String = spark.sparkContext.getConf.get("spark.ip.path")
    //    val ipRowRDD: RDD[String] = spark.sparkContext.textFile(ipPath)
    val ipRowRDD: RDD[String] = spark.sparkContext.textFile("file:///Users/zhaohaichao/workspace/javaspace/sparksql-train/data/ip.txt")

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

    ipRuleDF.show()

    val ipBro: Broadcast[DataFrame] = spark.sparkContext.broadcast(ipRuleDF)

    //    ipRuleDF.show(false)
    // json中的ip转换一下， 通过Spark SQL UDF函数
    import org.apache.spark.sql.functions._
    def getIpLog() = udf((ip: String) => {
      IPUtils.ip2Long(ip)
    })

    jsonDF = jsonDF.withColumn("ip_long", getIpLog()($"ip"))

    // 两个DF进行join，条件是jsonDF中的ip_long 是在规则ip中的范围内，ip_long between ... and ...

    val value: DataFrame = ipBro.value

    jsonDF.join(value, jsonDF("ip_long")
          .between(value("start_ip"), value("end_ip")))
          .show()

//    Thread.sleep(30000)

//    // ETL处理之后需要落地到Kudu
//
//    val KUDU_MASTER = "tencent"
//    //    val KUDU_MASTER = spark.sparkContext.getConf.get("spark.kudu.master")
//
//    val tableName = DateUtils.getTableName("ods", spark)
//    val schema: Schema = SchemaUtils.ODSSchema
//    val partitionId = "ip"
//    KuduUtils.sink(res, tableName, KUDU_MASTER, schema, partitionId)

  }
}
