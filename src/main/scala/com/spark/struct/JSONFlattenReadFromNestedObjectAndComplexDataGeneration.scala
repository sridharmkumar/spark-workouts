package com.spark.struct

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.{SparkConf, SparkContext}

object JSONFlattenReadFromNestedObjectAndComplexDataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    println()
    println("****** Raw Data ******")
    val rawData = spark.read.format("json").option("multiline", "true").load("resources/topping.json")
    rawData.show(5, truncate = false)
    rawData.printSchema()

    println()
    println("****** Flattening Data ******")
    val flattenData = rawData.select(
      col("id"),
      col("type"),
      col("name"),
      col("ppu"),
      col("batters.batter.id").alias("batter_id"),
      col("batters.batter.type").alias("batter_type"),
      col("topping.id").alias("topping_id"),
      col("topping.type").alias("topping_type")
    )
    flattenData.show(5, truncate = false)
    flattenData.printSchema()

    println()
    println("****** Complex Data Generation ******")
    val complexData = flattenData.select(
      col("id"),
      col("type"),
      col("name"),
      col("ppu"),
      struct(
        struct(
          col("batter_id").alias("id"),
          col("batter_type").alias("type")
        ).alias("batter")
      ).alias("batters"),
      struct(
        col("topping_id").alias("id"),
        col("topping_type").alias("type")
      ).alias("topping")
    )
    complexData.printSchema()
    complexData.show(5, truncate = false)
  }
}