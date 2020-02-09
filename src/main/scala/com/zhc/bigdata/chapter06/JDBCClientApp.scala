package com.zhc.bigdata.chapter06

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object JDBCClientApp {
  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = DriverManager.getConnection("jdbc:hive2://George:10000")
      pstmt = conn.prepareStatement("select * from test_db.emp")
      rs = pstmt.executeQuery()

      while (rs.next()) {
        println(rs.getObject(1) + " " +
          rs.getObject(2) + " " +
          rs.getObject(3) + " " +
          rs.getObject(4) + " " +
          rs.getObject(5) + " " +
          rs.getObject(6) + " " +
          rs.getObject(7) + " " +
          rs.getObject(8) + " "
        )
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    }
    finally {
      rs.close()
      pstmt.close()
      conn.close()
    }
  }

}
