package com.bdiiot.spark.phoenix.main

import org.apache.spark.sql.{ForeachWriter, SparkSession}

object HiveForeachWriter {
  def apply(): ForeachWriter[String] = {
    new HiveForeachWriter()
  }
}

class HiveForeachWriter() extends ForeachWriter[String] {
  var sparkSession: SparkSession = _

  override def open(partitionId: Long, version: Long): Boolean = {
    sparkSession = SparkSession.builder.master("local").enableHiveSupport().getOrCreate()
    true
  }

  override def process(value: String): Unit = {
    println(sparkSession.version.concat(value))
  }

  override def close(errorOrNull: Throwable): Unit = {
    sparkSession.close()
  }
}
