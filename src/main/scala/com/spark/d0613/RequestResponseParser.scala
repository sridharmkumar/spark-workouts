package com.spark.d0613

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.{SparkConf, SparkContext}

object RequestResponseParser {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()


    val jsonData = spark.read.format("json").option("multiline","true").load("file:///c://data//reqapi.json")
    println()
    println("****** Raw Data ******")
    jsonData.show(5, false)
    jsonData.printSchema()

    val flattenData = jsonData.select(col("data.avatar"),
    col("data.email"),
    col("data.first_name"),
    col("data.id"),
    col("data.last_name"),
    col("page"),
    col("per_page"),
    col("support.text"),
    col("support.url"),
    col("total"),
    col("total_pages"))

    println()
    println("****** Flatten Data ******")
    flattenData.show(5, false)
    flattenData.printSchema()

    val publishData = flattenData.select(
      struct(
        col("avatar"),
        col("email"),
        col("first_name"),
        col("id"),
        col("last_name")).alias("data"),
      col("page"),
      col("per_page"),
    struct(
      col("text"),
      col("url")).alias("support"),
    col("total"),
    col("total_pages"))
    println()
    println("****** Publish Data ******")
    publishData.show(5, false)
    publishData.printSchema()
  }

}
