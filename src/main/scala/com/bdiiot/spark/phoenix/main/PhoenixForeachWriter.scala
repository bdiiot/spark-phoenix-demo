package com.bdiiot.spark.phoenix.main

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import com.bdiiot.spark.phoenix.utils.Constant.{JDBC_URL, PHOENIX_DRIVER}
import org.apache.spark.sql.ForeachWriter

object PhoenixForeachWriter {
  def apply(): ForeachWriter[String] = {
    new PhoenixForeachWriter()
  }
}

class PhoenixForeachWriter() extends ForeachWriter[String] {
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
    var sql = s"upsert into test.test_phoenix values ($key,'$value')"
    statement.executeUpdate(sql)
    connection.commit()

    sql = "select * from test.test_phoenix"
    resultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      println(resultSet.getObject(2).toString)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    try {
      resultSet.close()
    } catch {
      case ex: Exception =>
        println(s"close resultSet failed, msg=$ex")
    }
    try {
      statement.close()
    } catch {
      case ex: Exception =>
        println(s"close statement failed, msg=$ex")
    }
    try {
      connection.close()
    } catch {
      case ex: Exception =>
        println(s"close connection failed, msg=$ex")
    }
  }
}
