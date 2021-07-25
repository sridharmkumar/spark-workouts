package com.spark.array

import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}

object JSONFlattenArrayAndDataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("json").option("multiline","true").load("src/main/resources/array.json")
    println("****** Raw Data ******")
    rawData.printSchema()
    rawData.show(5, truncate = false)

    val flattenData = rawData.select(
      col("first_name"),
      col("last_name"),
      col("address.temporary_address").alias("temporary_address"),
      col("address.permanent_address").alias("permanent_address"),
      col("students")
    ).withColumn("students", functions.explode(col("students")))
    println
    println("****** Flatten Data ******")
    flattenData.printSchema()
    flattenData.show(5, truncate = false)

    val aggregateData = flattenData.groupBy(
      col("first_name"),
      col("last_name"),
      col("temporary_address"),
      col("permanent_address")
    ).agg(collect_list(col("students")).alias("students"))

    val complexData =  aggregateData.select(
        col("first_name"),
        col("last_name"),
        struct(
          col("temporary_address").alias("temporary_address"),
          col("permanent_address").alias("permanent_address")
        ).alias("address"),
      col("students")
      )
    println
    println("****** Complex Data ******")
    complexData.printSchema()
    complexData.show(5, truncate = false)

  }
}
