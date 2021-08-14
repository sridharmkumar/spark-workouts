package com.spark.elasticsearch

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WriteToElasticSearch {

  def b2s(a: Array[Byte]): String = new String(a)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val ssc = new StreamingContext(conf, Seconds(2))
    val stream = KinesisUtils.createStream(
      ssc,
      "<<kinesisAppName>>",
      "<<streamName>>",
      "<<endpointUrl>>",
      "<<regionName>>",
      InitialPositionInStream.TRIM_HORIZON,
      Seconds(1),
      StorageLevel.MEMORY_AND_DISK_2)

    val finalStream = stream.map(x => b2s(x))
    finalStream.print()

    finalStream.foreachRDD(x =>
      if (!x.isEmpty()) {
        val df = spark.read.json(x)
        val username = df
          .withColumn("results", explode(col("results")))
          .selectExpr("results.user.username as name")

        username.write.format("org.elasticsearch.spark.sql")
          .option("es.nodes.wan.only", "true")
          .option("spark.es.net.http.auth.user", "<<username>>")
          .option("spark.es.net.http.auth.pass", "<<password>>")
          .option("es.net.http.auth.user", "<<username>>")
          .option("es.net.http.auth.pass", "<<password>>")
          .option("es.port", "443")
          .option("es.nodes", "<<Elastic Search URL>>")
          .mode("append")
          .save("index/table")

        println("data written to elastic search")
      })

    ssc.start()
    ssc.awaitTermination()
  }

}
