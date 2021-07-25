package com.spark.array

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.{SparkConf, SparkContext}

object JSONComplexDataFlattenArray {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val rawData = spark.read.format("json").option("multiline", "true").load("src/main/resources/complexarray.json")
    println("****** Raw Data - Schema ******")
    rawData.printSchema()
    println("****** Raw Data - Data ******")
    rawData.show(5, truncate = false)

    val flattenData = rawData.select(
      col("title"),
      col("first_name"),
      col("last_name"),
      col("address.permanent_address"),
      col("address.temporary_address"),
      col("students")
    )
    flattenData.printSchema()
    flattenData.show(5, truncate = false)

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
      col("students.user.components").alias("students_components")
    )

    val explodeComponentData = explodeStudentData.withColumn("students_components", explode(col("students_components")))
    explodeComponentData.printSchema()
    explodeComponentData.show(5, truncate = false)
  }
}
