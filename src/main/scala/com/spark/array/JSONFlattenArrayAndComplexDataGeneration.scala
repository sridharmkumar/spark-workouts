package com.spark.array

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_list, explode, struct}
import org.apache.spark.{SparkConf, SparkContext}

object JSONFlattenArrayAndComplexDataGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("json").option("multiline", "true").load("src/main/resources/complexarray.json")
    println("****** Raw Data ******")
    rawData.printSchema()
    rawData.show(5, truncate = false)

    println("****** Flattening Data ******")
    val flattenData = rawData.select(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("address.permanent_address"),
      col("address.temporary_address"),
      col("students")
    )

    println("****** Exploding Students ******")
    val explodeStudentData = flattenData.withColumn("students", explode(col("students"))).select(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("permanent_address"),
      col("temporary_address"),
      col("students.user.address.permanent_address").alias("user_permanent_address"),
      col("students.user.address.temporary_address").alias("user_temporary_address"),
      col("students.user.gender").alias("user_gender"),
      col("students.user.name.first_name").alias("user_first_name"),
      col("students.user.name.last_name").alias("user_last_name"),
      col("students.user.name.title").alias("user_title"),
      col("students.user.components").alias("components")
    )

    println("****** Exploding Components ******")
    val explodeComponentData = explodeStudentData.withColumn("students_components", explode(col("components")))
    println("****** Final Flatten Data ******")
    explodeComponentData.printSchema()
    explodeComponentData.show(5, truncate = false)

    println("****** Aggregating Component ******")
    val aggregateComponentData = explodeComponentData.groupBy(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("permanent_address"),
      col("temporary_address"),
      col("user_permanent_address"),
      col("user_temporary_address"),
      col("user_gender"),
      col("user_title"),
      col("user_first_name"),
      col("user_last_name")
    ).agg(collect_list(col("students_components")).alias("components"))

    println("****** Aggregating Students ******")
    val finalData = aggregateComponentData.groupBy(
      col("title"),
      col("first_name"),
      col("last_name"),
      struct(
        col("permanent_address"),
        col("temporary_address")
      ).alias("address")
    ).agg(collect_list(struct(struct(
      col("user_gender").alias("gender"),
      col("components"),
      struct(
        col("user_first_name").alias("first_name"),
        col("user_last_name").alias("last_name"),
        col("user_title").alias("title")
      ).alias("name")
    ).alias("user")).alias("user")).alias("students"))

    println("****** Re-Generated - Schema ******")
    finalData.printSchema()
    println("****** Re-Generated - Data ******")
    finalData.show(5, truncate = false)
  }
}