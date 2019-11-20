package com.bdiiot.spark.phoenix.main

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.bdiiot.spark.phoenix.utils.Constant._
import com.bdiiot.spark.phoenix.utils.SparkHelper
import org.apache.spark.sql
import org.apache.spark.sql.ForeachWriter


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
      .foreach(new ForeachWriter[String] {
        var connection: Connection = _
        var statement: Statement = _
        var resultSet: ResultSet = _

        override def open(partitionId: Long, version: Long): Boolean = {
          Class.forName(PHOENIX_DRIVER)
          connection = DriverManager.getConnection(JDBC_URL)
          statement = connection.createStatement
          true
        }

        override def process(value: String): Unit = {
          val key = System.nanoTime().toString.reverse
          var sql = s"upsert into test.test_phoenix values (${key},'${value}')"
          statement.executeUpdate(sql)
          connection.commit()

          sql = "select * from test.test_phoenix"
          val resultSet = statement.executeQuery(sql)
          while (resultSet.next()) {
            println(resultSet.getObject(2).toString)
          }
        }

        override def close(errorOrNull: Throwable): Unit = {
          try {
            resultSet.close()
          } catch {
            case ex: Exception => {
              println(s"close resultSet failed, msg=$ex")
            }
          }
          try {
            statement.close()
          } catch {
            case ex: Exception => {
              println(s"close statement failed, msg=$ex")
            }
          }
          try {
            connection.close()
          } catch {
            case ex: Exception => {
              println(s"close connection failed, msg=$ex")
            }
          }
        }
      })
      .outputMode(OUTPUT_MODE)
      .option("checkpointLocation", PATH_CHECKPOINT + "demo")
      .start()

    query.awaitTermination()
    SparkHelper.close
  }
}
