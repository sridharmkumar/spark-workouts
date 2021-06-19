package com.spark.array

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.{SparkConf, SparkContext}

object ComplexJSONFlattenArrayAndDataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("json").option("multiline","true").load("src/main/resources/complexarray.json")
    println("****** Raw Data ******")
    rawData.printSchema()
    rawData.show(5, truncate = false)

    val explodeData = rawData.select(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("address.permanent_address").alias("permanent_address"),
      col("address.temporary_address").alias("temporary_address"),
      col("students")
    ).withColumn("students", explode(col("students")))

    val flattenData = explodeData.select(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("permanent_address"),
      col("temporary_address"),
      col("students.user.address.permanent_address").alias("user_permanent_address"),
      col("students.user.address.temporary_address").alias("user_temporary_address"),
      col("students.user.gender"),
      col("students.user.name.title").alias("user_title"),
      col("students.user.name.first_name").alias("user_first_name"),
      col("students.user.name.last_name").alias("user_last_name")
    )
    flattenData.printSchema()
    flattenData.show(5, truncate = false)

    /*val flattenData = rawData.select(
      col("first_name"),
      col("second_name"),
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
      col("second_name"),
      col("temporary_address"),
      col("permanent_address")
    ).agg(collect_list(col("students")).alias("students"))

    val complexData =  aggregateData.select(
        col("first_name"),
        col("second_name"),
        struct(
          col("temporary_address").alias("temporary_address"),
          col("permanent_address").alias("permanent_address")
        ).alias("address"),
      col("students")
      )
    println
    println("****** Complex Data ******")
    complexData.printSchema()
    complexData.show(5, truncate = false)*/
  }
}
