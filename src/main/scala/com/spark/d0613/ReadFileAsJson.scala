package com.spark.d0613

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ReadFileAsJson {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val jsonData = spark.read.json("file:///c://data//devices.json")
    val jsonSchema = jsonData.schema
    println()
    println("***************************")
    println(jsonSchema)
    println("***************************")
  }

}
