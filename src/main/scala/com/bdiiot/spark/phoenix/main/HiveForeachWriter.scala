package com.bdiiot.spark.phoenix.main

import org.apache.spark.sql.{ForeachWriter, SparkSession}

object HiveForeachWriter {
  def apply(sparkSession: SparkSession): ForeachWriter[String] = {
    new HiveForeachWriter(sparkSession)
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
