package com.spark.array

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.{SparkConf, SparkContext}

object JSONFlattenAndComplexDataGenerationFromURL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val url = "https://randomuser.me/api/0.8/?results=50"

    println()
    println("*************************************************")
    println("Retrieving data from url - " + url)
    val result = scala.io.Source.fromURL(url).mkString
    println("Data retrieval successfully completed.")

    val rdd = sc.parallelize(List(result))
    val jsonDataFrame = spark.read.json(rdd)
    jsonDataFrame.show(5, truncate = false)
    jsonDataFrame.printSchema()

    val explodeData=jsonDataFrame.withColumn("results", explode(col("results")))
    val flattenData=explodeData.select(
      col("nationality"),
      //col("results.user.BSN"),
      col("results.user.cell"),
      col("results.user.dob"),
      col("results.user.email"),
      col("results.user.gender"),
      col("results.user.location.city"),
      col("results.user.location.state"),
      col("results.user.location.street"),
      col("results.user.location.zip"),
      col("results.user.md5"),
      col("results.user.name.first"),
      col("results.user.name.last"),
      col("results.user.name.title"),
      col("results.user.password"),
      col("results.user.phone"),
      col("results.user.picture.large"),
      col("results.user.picture.medium"),
      col("results.user.picture.thumbnail"),
      col("results.user.registered"),
      col("results.user.salt"),
      col("results.user.sha1"),
      col("results.user.sha256"),
      col("results.user.username"),
      col("seed"),
      col("version")
    )
    flattenData.printSchema();
    println("flatten data count : "+flattenData.count())
  }
}
