package com.bdiiot.spark.phoenix.main

import com.bdiiot.spark.phoenix.utils.Constant._
import com.bdiiot.spark.phoenix.utils.SparkHelper
import org.apache.spark.sql


object SparkPhoenixMain {
  def main(args: Array[String]): Unit = {
    if (SECURITY == "SASL_PLAINTEXT") {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("java.security.auth.login.config", "/tmp/kafka_bigdata_jaas.conf")
    }

    val spark = SparkHelper.getSparkSession()

    val kafkaSource: sql.DataFrame = spark
      .readStream
      .format(KAFKA_SOURCE)
      .option("kafka.bootstrap.servers", BROKERS)
      .option("subscribe", TOPICS)
      .option("kafka.security.protocol", SECURITY)
      .option("startingOffsets", OFFSETS)
      .load()

    import spark.implicits._
    val kafkaSourceString = kafkaSource.selectExpr("CAST(value AS STRING)").as[String]

    val query = kafkaSourceString.writeStream
      .foreach(HiveForeachWriter.apply())
      .outputMode(OUTPUT_MODE)
      .option("checkpointLocation", PATH_CHECKPOINT + "demo")
      .start()

    query.awaitTermination()
    SparkHelper.close
  }
}
