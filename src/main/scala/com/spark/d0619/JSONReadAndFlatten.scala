package com.spark.d0619

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}

object JSONReadAndFlatten {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("json").option("multiline", "true").load("file:///C:/data/topping.json")
    println()
    println("****** Raw Data ******")
    rawData.show(5, false)
    rawData.printSchema()

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
    println()
    println("****** Flatten Data ******")
    flattenData.show(5, false)
    flattenData.printSchema()

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
    println()
    println("****** Complex Data ******")
    complexData.printSchema()
    complexData.show(5, false)
  }

}
