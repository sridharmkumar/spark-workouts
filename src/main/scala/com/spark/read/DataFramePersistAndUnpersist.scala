package com.spark.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, SparkContext}

object DataFramePersistAndUnpersist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val columnList = List("category", "product", "txnno", "txndate", "amount", "city", "state", "spendby", "custno")
    val csvDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/file3.txt").select(columnList.map(col): _*)
    csvDataFrame.persist()
    spark.time(csvDataFrame.createOrReplaceTempView("tmpcsvdata01"))
    spark.time(spark.sql("select * from tmpcsvdata01").show(5))

    spark.time(csvDataFrame.createOrReplaceTempView("tmpcsvdata02"))
    spark.time(spark.sql("select * from tmpcsvdata02").show(5))
    csvDataFrame.unpersist()
  }
}
