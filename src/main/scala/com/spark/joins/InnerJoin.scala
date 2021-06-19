package com.spark.joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

object InnerJoin {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val dataSet1 = spark.read.format("csv").option("header", "true").load("src/main/resources/dataset1.csv")
    val dataSet2 = spark.read.format("csv").option("header", "true").load("src/main/resources/dataset2.csv")

    println("***** Dataset 1 *****")
    dataSet1.show(5, truncate = false)

    println
    println("***** Dataset 2 *****")
    dataSet2.show(5, truncate = false)

    val innerJoin = dataSet1.join(dataSet2, dataSet1("txnno") === dataSet2("txn_number"), "inner").drop(col("txn_number"))
    println
    println("***** Inner Join Result *****")
    innerJoin.show(10, truncate = false)
  }
}