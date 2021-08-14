package com.spark.streaming

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KinesisStreaming {

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
      "<<endpointUtl>>",
      "<<regionName>>",
      InitialPositionInStream.TRIM_HORIZON,
      Seconds(1),
      StorageLevel.MEMORY_AND_DISK_2)

    val finalStream = stream.map(x => b2s(x))
    finalStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def b2s(a: Array[Byte]): String = new String(a)
}
