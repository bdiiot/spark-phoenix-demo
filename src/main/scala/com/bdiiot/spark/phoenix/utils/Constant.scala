package com.bdiiot.spark.phoenix.utils

object Constant {
  final val HDFS = "hdfs://h11.bdiiot.com"
  final val STOP_FILE = "/tmp/spark_phoenix_demo_stop"

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

  final val JDBC_URL = "jdbc:phoenix:h11.bdiiot.com,h12.bdiiot.com,h13.bdiiot.com:2181:/hbase-secure:hbase-bdiiot@BDIIOT.COM:/etc/security/keytabs/hbase.headless.keytab"
  final val PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver"
}
