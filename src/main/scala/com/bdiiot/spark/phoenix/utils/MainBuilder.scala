package com.bdiiot.spark.phoenix.utils

import java.sql.{Connection, DriverManager}

import com.bdiiot.spark.phoenix.utils.Constant._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class MainBuilder extends Serializable {

  def buildSparkSession: SparkSession = {
    @transient lazy val conf: SparkConf = new SparkConf().setAppName(APP_NAME)
    @transient lazy val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }

  def buildPhoenixConnection: Connection = {
    Class.forName(PHOENIX_DRIVER)
    @transient lazy val connection = DriverManager.getConnection(JDBC_URL)
    connection
  }
}
