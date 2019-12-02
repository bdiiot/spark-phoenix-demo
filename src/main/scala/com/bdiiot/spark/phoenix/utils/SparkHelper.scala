package com.bdiiot.spark.phoenix.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {
  private var singleSparkSession: SparkSession = _

  def getSparkSession: SparkSession = {
    synchronized {
      if (singleSparkSession == null) {
        val conf = new SparkConf()
          .setAppName("spark_phoenix_demo")

        singleSparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
        singleSparkSession.sparkContext.setCheckpointDir(Constant.PATH_CHECKPOINT + "spark")
      }
    }
    singleSparkSession
  }

  def close(): Unit = {
    if (singleSparkSession != null) {
      try {
        singleSparkSession.close()
      } catch {
        case ex: Exception =>
          println(s"close singled spark session failed, msg=$ex")
      }
    }
  }
}
