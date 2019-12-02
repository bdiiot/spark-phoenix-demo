package com.bdiiot.spark.phoenix.main

import com.bdiiot.spark.phoenix.utils.SparkHelper
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object HiveForeachWriter {
  def apply(): ForeachWriter[String] = {
    new HiveForeachWriter()
  }
}

class HiveForeachWriter() extends ForeachWriter[String] {
  var sparkSession: SparkSession = _

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(value: String): Unit = {
    sparkSession = SparkHelper.getLocalSession
    println(sparkSession.version.concat(value))
  }

  override def close(errorOrNull: Throwable): Unit = {
    sparkSession.close()
  }
}
