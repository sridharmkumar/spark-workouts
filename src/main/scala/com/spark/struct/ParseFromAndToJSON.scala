package com.spark.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object ParseFromAndToJSON {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val weblogschema = StructType(Array(
      StructField("device_id", StringType, nullable = true),
      StructField("device_name", StringType, nullable = true),
      StructField("humidity", StringType, nullable = true),
      StructField("lat", StringType, nullable = true),
      StructField("long", StringType, nullable = true),
      StructField("scale", StringType, nullable = true),
      StructField("temp", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true),
      StructField("zipcode", StringType, nullable = true)))

    val rawData = spark.read.format("csv").option("delimiter", "~").load("file:///c://data//devices.json")
    val jsonData = rawData.withColumn("json", from_json(col("_c0"), weblogschema)).select("json.*")
    println("***** from_json *****")
    jsonData.show(5, truncate = false)
    jsonData.printSchema()

    val originalData = jsonData.select(to_json(
      struct(
        col("device_id"),
        col("device_name"),
        col("humidity"),
        col("lat"),
        col("long"),
        col("scale"),
        col("temp"),
        col("timestamp"),
        col("zipcode")
      )).alias("_c0"))
    println
    println("***** to_json *****")
    originalData.show(5, truncate = false)
    originalData.printSchema()

  }
}
