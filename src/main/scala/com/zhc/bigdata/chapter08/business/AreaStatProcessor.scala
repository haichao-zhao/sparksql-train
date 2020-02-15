package com.zhc.bigdata.chapter08.business

import com.zhc.bigdata.chapter08.`trait`.DataProcess
import com.zhc.bigdata.chapter08.utils.{DateUtils, KuduUtils, SQLUtils, SchemaUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AreaStatProcessor extends DataProcess {
  override def process(spark: SparkSession): Unit = {

    val KUDU_MASTER = spark.sparkContext.getConf.get("spark.kudu.master")
    val sourceTableName = DateUtils.getTableName("ods", spark)
    val toTableName = DateUtils.getTableName("area_stat", spark)

    val odsDF: DataFrame = spark.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", KUDU_MASTER)
      .option("kudu.table", sourceTableName)
      .load()

    odsDF.createOrReplaceTempView("ods")

    val resTmp: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP1)
    resTmp.createOrReplaceTempView("area_tmp")

    val res: DataFrame = spark.sql(SQLUtils.AREA_SQL_STEP2)

    val partitionId = "provincename"

    KuduUtils.sink(res, toTableName, KUDU_MASTER, SchemaUtils.AREASchema, partitionId)

  }
}
