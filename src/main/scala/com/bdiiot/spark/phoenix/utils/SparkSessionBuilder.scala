package com.bdiiot.spark.phoenix.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder extends Serializable {
  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf()
      .setAppName("spark_phoenix_demo")
    @transient lazy val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
    spark
  }
}
