package com.zhc.bigdata.chapter08.utils

object IPUtils {

  /**
    * 将字符串转成10进制
    *
    * @param ip
    */
  def ip2Long(ip: String) = {
    val splits: Array[String] = ip.split("[.]")
    var ipNum = 0l

    for (i <- 0 until (splits.length)) {
      ipNum = splits(i).toLong | ipNum << 8L
    }

    ipNum
  }

  def main(args: Array[String]): Unit = {
    println(ip2Long("192.168.1.104"))
  }

}
