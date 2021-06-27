package com.spark.aws

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object AWSIntegration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key", "<<your AWS access key>>")
      .config("fs.s3a.secret.key", "<<your AWS secret key>>").getOrCreate()


    val rawData = spark.read.parquet("<<S3 URL>>") /* e.g:- s3a:// */
    rawData.show()
  }
}
