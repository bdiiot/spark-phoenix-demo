package com.bdiiot.spark.phoenix.utils

object Constant {
  final val HDFS = "hdfs://h11.bdiiot.com"
  final val PATH_CHECKPOINT = HDFS + "/tmp/spark_phoenix/"
  final val OUTPUT_MODE = "update"

  final val CONSOLE_SOURCE = "console"

  final val KAFKA_SOURCE = "kafka"
  final val BROKERS = "h11.bdiiot.com:6667,h12.bdiiot.com:6667,h13.bdiiot.com:6667"
  final val TOPICS = "test_phoenix"
  // kafka.security [PLAINTEXT, SASL_PLAINTEXT]
  final val SECURITY = "SASL_PLAINTEXT"
  // auto.offset.reset [latest, earliest, none]
  final val OFFSETS = "earliest"
}
