package com.bdiiot.spark.phoenix.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkHelper {
  private var singleSparkSession: SparkSession = _
  private var localSparkSession: SparkSession = _

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

  def getLocalSession: SparkSession = {
    synchronized {
      if (localSparkSession == null) {
        localSparkSession = SparkSession.
          builder.
          master("local").
          enableHiveSupport().
          getOrCreate()

        localSparkSession.sparkContext.setCheckpointDir(Constant.PATH_CHECKPOINT + "spark_local")
      }
    }
    localSparkSession
  }

  def localClose(): Unit = {
    if (localSparkSession != null) {
      try {
        localSparkSession.close()
      } catch {
        case ex: Exception =>
          println(s"close local spark session failed, msg=$ex")
      }
    }
  }
}
