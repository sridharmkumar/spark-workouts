package com.spark.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ReadJSONSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val jsonData = spark.read.json("src/main/resources/devices.json")
    val jsonSchema = jsonData.schema
    println()
    println("***************************")
    println(jsonSchema)
    println("***************************")
  }
}
