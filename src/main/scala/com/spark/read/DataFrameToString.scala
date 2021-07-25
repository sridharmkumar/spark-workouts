package com.spark.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameToString {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val data = spark.read.format("csv").option("header","true").load("src/main/resources/txns")
    data.createOrReplaceTempView("txns")
    val maxDataFrame = spark.sql("select max(txnno) from txns")
    maxDataFrame.show(false)
    val maxValue = maxDataFrame.collect().map(x=>x.mkString("")).mkString("").toInt
    println("Max Value is " + maxValue)
  }
}
