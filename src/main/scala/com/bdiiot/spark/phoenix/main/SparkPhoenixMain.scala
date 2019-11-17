package com.bdiiot.spark.phoenix.main

import com.bdiiot.spark.phoenix.utils.Constant._
import com.bdiiot.spark.phoenix.utils.SparkHelper
import org.apache.spark.sql
import org.apache.spark.sql.ForeachWriter

object SparkPhoenixMain {
  def main(args: Array[String]): Unit = {
    if (SECURITY == "SASL_PLAINTEXT") {
      System.setProperty("java.security.krb5.conf", "/etc/krb5.conf")
      System.setProperty("java.security.auth.login.config", "/etc/kafka/conf/kafka_client_jaas.conf")
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

    // output to console
    //    val query = kafkaSourceString
    //      .writeStream
    //      .format(CONSOLE_SOURCE)
    //      .outputMode(OUTPUT_MODE)
    //      .option("checkpointLocation", PATH_CHECKPOINT + "spark_phoenix_demo")
    //      .start()

    val query = kafkaSourceString.writeStream
      .foreach(new ForeachWriter[String] {
        override def open(partitionId: Long, version: Long): Boolean = ???

        override def process(value: String): Unit = ???

        override def close(errorOrNull: Throwable): Unit = ???
      })
      .outputMode(OUTPUT_MODE)
      .option("checkpointLocation", PATH_CHECKPOINT + "spark_phoenix_demo")
      .start()

    query.awaitTermination()
    SparkHelper.close
  }
}
