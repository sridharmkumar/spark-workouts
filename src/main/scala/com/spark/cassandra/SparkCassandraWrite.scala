package com.spark.cassandra

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkCassandraWrite {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder()
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    val txnData = spark.read.format("csv").option("header", "true").load("src/main/resources/txns")
    txnData.show(5)

    txnData.write.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "mykeyspace",
        "table" -> "txndata"
      )).save()

    println("Txn data written to Cassandra successfully!!!")
  }
}
