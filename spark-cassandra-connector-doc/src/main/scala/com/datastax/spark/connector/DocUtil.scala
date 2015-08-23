package com.datastax.spark.connector

import com.datastax.spark.connector.util.ConfigCheck

object DocUtil {
  def main(args: Array[String]) {
    println(ConfigCheck.validStaticProperties)
  }
}
