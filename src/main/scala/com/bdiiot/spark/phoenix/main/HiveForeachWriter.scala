package com.bdiiot.spark.phoenix.main

import com.bdiiot.spark.phoenix.utils.SparkSessionBuilder
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object HiveForeachWriter extends SparkSessionBuilder {
  def apply(): ForeachWriter[String] = {
    new HiveForeachWriter(buildSparkSession)
  }
}

class HiveForeachWriter(sparkSession: SparkSession) extends ForeachWriter[String] {

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(value: String): Unit = {
    println(sparkSession.version.concat(value))
  }

  override def close(errorOrNull: Throwable): Unit = {
  }
}
