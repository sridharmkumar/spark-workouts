package com.spark.read

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_date
import org.apache.spark.{SparkConf, SparkContext}

object NullHandling {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/nulldata.txt")
    data.printSchema()

    data.na.fill("NA").na.fill(0).withColumn("current_date", current_date()).show(false)

  }
}
