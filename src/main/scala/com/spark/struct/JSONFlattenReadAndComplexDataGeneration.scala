package com.spark.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.{SparkConf, SparkContext}

object JSONFlattenReadAndComplexDataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    println("***** Raw Data *****")
    val jsonData = spark.read.format("json").option("", "").load("file:///C://data//picture.json")
    jsonData.show()
    jsonData.printSchema()

    println
    println("***** Flatten Data *****")
    val flattenData = jsonData.select(
      col("id"),
      col("image.height").alias("image_height"),
      col("image.url").alias("image_url"),
      col("image.width").alias("image_width"),
      col("name"),
      col("thumbnail.height").alias("thumbnail_height"),
      col("thumbnail.url").alias("thumbnail_url"),
      col("thumbnail.width").alias("thumbnail_width"),
      col("type")
    )
    flattenData.show()
    flattenData.printSchema()

    println
    println("***** Data Generation *****")
    val generationData = flattenData.select(
      col("id"),
      struct(
        col("image_height").alias("height"),
        col("image_url").alias("url"),
        col("image_height").alias("height")).alias("image"),
      col("name"),
      struct(
        col("thumbnail_height").alias("height"),
        col("thumbnail_url").alias("url"),
        col("thumbnail_height").alias("height")).alias("thumbnail"),
      col("type")
    )
    generationData.show()
    generationData.printSchema()
  }
}
