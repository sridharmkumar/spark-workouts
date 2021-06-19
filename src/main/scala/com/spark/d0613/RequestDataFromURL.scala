package com.spark.d0613

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RequestDataFromURL  {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val url = "https://randomuser.me/api/0.8/?results=10"
    println("Retrieving data from url - " + url)
    val result = scala.io.Source.fromURL(url).mkString
    println("Data retrieval successfully completed.")

    val rdd = sc.parallelize(List(result))
    val jsonDataFrame = spark.read.json(rdd)
    jsonDataFrame.show(5, truncate = false)

  }

}
