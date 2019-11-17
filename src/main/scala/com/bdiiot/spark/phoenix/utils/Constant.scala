package com.bdiiot.spark.phoenix.utils

object Constant {
  final val HDFS = "hdfs://h11.bdiiot.com"
  final val PATH_CHECKPOINT = HDFS + "/tmp/spark_phoenix_demo/"
  final val OUTPUT_MODE = "update"

  final val CONSOLE_SOURCE = "console"

  final val KAFKA_SOURCE = "kafka"
  final val BROKERS = "kafka"
  final val TOPICS = "kafka"
  // kafka.security [PLAINTEXT, SASL_PLAINTEXT]
  final val SECURITY = "SASL_PLAINTEXT"
  // auto.offset.reset [latest, earliest, none]
  final val OFFSETS = "earliest"
}
