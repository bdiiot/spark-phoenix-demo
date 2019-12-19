package com.bdiiot.spark.phoenix.utils

import com.bdiiot.spark.phoenix.utils.Constant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

object SparkHelper {
  private var singleSparkSession: SparkSession = _

  def getSparkSession(): SparkSession = {
    synchronized {
      if (singleSparkSession == null) {
        val conf = new SparkConf()
          .setAppName("spark_phoenix_demo")

        singleSparkSession = SparkSession.builder()
          .config(conf)
          .enableHiveSupport()
          .getOrCreate()
        singleSparkSession.sparkContext.setCheckpointDir(PATH_CHECKPOINT + "spark")
      }
    }
    singleSparkSession
  }

  def close(): Unit = {
    if (singleSparkSession != null) {
      try {
        singleSparkSession.close()
      } catch {
        case ex: Exception => {
          println(s"close singled spark session failed, msg=$ex")
        }
      }
    }
  }

  def stopByMarkFile(query: StreamingQuery): Unit = {
    val intervalMills = 10 * 1000
    var isStop = false
    while (!isStop) {
      isStop = query.awaitTermination(intervalMills)
      if (!isStop && isExistsMarkFile(STOP_FILE)) {
        val second = 2
        println(s"stop after $second seconds")
        Thread.sleep(second * 1000)
        query.stop()
      }
    }
  }

  def isExistsMarkFile(hdfsFilePath: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfsFilePath)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

}
