package com.bdiiot.spark.phoenix.main

import com.bdiiot.spark.phoenix.utils.Constant._
import com.bdiiot.spark.phoenix.utils.{SparkHelper, SparkSessionBuilder}
import org.apache.spark.sql
import org.apache.spark.sql.ForeachWriter


object SparkPhoenixMain extends SparkSessionBuilder {
  def main(args: Array[String]): Unit = {
    if (SECURITY == "SASL_PLAINTEXT") {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("java.security.auth.login.config", "/tmp/kafka_bigdata_jaas.conf")
    }

    val spark = buildSparkSession

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
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, version: Long): Boolean = true

        override def process(value: String): Unit = {
          println(spark.version.concat(value))
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .outputMode(OUTPUT_MODE)
      .option("checkpointLocation", PATH_CHECKPOINT + "demo")
      .start()

    query.awaitTermination()
    SparkHelper.close()
  }
}
