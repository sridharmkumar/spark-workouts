package com.spark.filter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object ListFiltering {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_app").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("csv").option("header", "true").load("src/main/resources/file01.txt")
    val filterData = spark.read.format("csv").option("header", "true").load("src/main/resources/file02.txt")

    val ids = filterData.select(col("id")).collect().map(_ (0)).toList
    //val ids = -filterData.select(col("id")).rdd.map(r => r(0)).collect.toList

    println()
    println("IDs to be removed " + ids)

    val result = rawData.filter(!col("id").isin(ids: _*))
    result.show()

  }

}
