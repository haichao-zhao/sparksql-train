package com.zhc.bigdata.chapter05

import com.typesafe.config.ConfigFactory

object ParamsApp {

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    println(conf.getString("db.default.url"))
  }
}
