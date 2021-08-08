package com.spark.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCassandraIntegration {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    val empData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "mykeyspace",
        "table" -> "emp")).load()
    empData.show()
  }
}
