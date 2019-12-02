package com.bdiiot.spark.phoenix.utils

import com.bdiiot.spark.phoenix.utils.Constant._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkSessionBuilder extends Serializable {

  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf().setAppName(APP_NAME)
    @transient lazy val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

}
