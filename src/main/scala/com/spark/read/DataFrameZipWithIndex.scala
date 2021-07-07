package com.spark.read

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameZipWithIndex {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_app").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val df = spark.read.format("csv").option("header","true").load("src/main/resources/txns")
    val result = addColumnIndex(spark, df)
    result.show()
  }

  def addColumnIndex(spark: SparkSession,df: DataFrame) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("index", LongType, false)))
  }
}
