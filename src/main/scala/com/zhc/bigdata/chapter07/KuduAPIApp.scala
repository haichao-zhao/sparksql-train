package com.zhc.bigdata.chapter07

import java.util

import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.kudu.client._

object KuduAPIApp {


  def main(args: Array[String]): Unit = {

    val KUDU_MASTER = "hadoop000"
    val client: KuduClient = new KuduClient.KuduClientBuilder(KUDU_MASTER).build()

    val table_name = "pk"
    val new_table_name = "new_pk"

    createTable(client, table_name)

    //    insertRows(client, new_table_name)

    //    updateRow(client, table_name)
    //        queryTable(client, new_table_name)

    //    deleteTable(client, table_name)


    //    reTableName(client, table_name, new_table_name)
  }

  /**
    * 创建表
    *
    * @param client
    * @param table_name
    */
  def createTable(client: KuduClient, table_name: String): Unit = {

    import scala.collection.JavaConverters._

    val list = List(
      new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("location", Type.STRING).build(),
      new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build()
    ).asJava

    val schema = new Schema(list)

    val linkedList = new util.LinkedList[String]()
    linkedList.add("id")

    val options = new CreateTableOptions()
    options.setNumReplicas(1)
    options.addHashPartitions(linkedList, 3)

    client.createTable(table_name, schema, options)

  }

  /**
    * 插入数据
    *
    * @param client
    * @param table_name
    */
  def insertRows(client: KuduClient, table_name: String): Unit = {

    val table: KuduTable = client.openTable(table_name)
    val session: KuduSession = client.newSession()

    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)

    val DATA_COMMIT_SIZE = 5000
    //    session.setFlushInterval(500) //设置刷新间隔 单位毫秒
    session.setMutationBufferSpace(DATA_COMMIT_SIZE) //设置缓存区条数

    var count = 0

    for (i <- 1 to 10000) {
      val insert: Insert = table.newInsert()
      val row: PartialRow = insert.getRow
      row.addString("word", s"zhc-$i")
      row.addInt("cnt", i)

      session.apply(insert)

      count += 1

      if (count > DATA_COMMIT_SIZE / 2) {
        //设置缓存区条数为总条数的一半时提交

        session.flush()
        count = 0
      }

    }

    session.flush()
    session.close()

  }

  /**
    * 删除表
    *
    * @param client
    * @param table_name
    */
  def deleteTable(client: KuduClient, table_name: String): Unit = {
    client.deleteTable(table_name)
  }

  /**
    * 查询表
    *
    * @param client
    * @param table_name
    */
  def queryTable(client: KuduClient, table_name: String): Unit = {

    val table: KuduTable = client.openTable(table_name)

    val scanner: KuduScanner = client.newScannerBuilder(table).build()

    while (scanner.hasMoreRows) {
      val iterator: RowResultIterator = scanner.nextRows()
      while (iterator.hasNext) {
        val result: RowResult = iterator.next()

        val id: Int = result.getInt("id")
        val location: String = result.getString("location")
        val name: String = result.getString("name")

        println(id + "|" + location + "|" + name)

      }
    }
  }

  /**
    * 修改表数据
    *
    * @param client
    * @param table_name
    */
  def updateRow(client: KuduClient, table_name: String): Unit = {

    val table: KuduTable = client.openTable(table_name)
    val session: KuduSession = client.newSession()

    val update: Update = table.newUpdate()
    val row: PartialRow = update.getRow()
    row.addString("word", s"pk-10")
    row.addInt("cnt", 999)

    session.apply(update)
    session.close()
  }

  /**
    * 修改表名
    *
    * @param client
    * @param old_table_name
    * @param new_table_name
    */
  def reTableName(client: KuduClient, old_table_name: String, new_table_name: String): Unit = {

    val options = new AlterTableOptions()
    options.renameTable(new_table_name)
    client.alterTable(old_table_name, options)
  }


}
