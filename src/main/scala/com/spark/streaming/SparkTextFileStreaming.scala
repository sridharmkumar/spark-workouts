package com.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkTextFileStreaming {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]").set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")

    val ssc = new StreamingContext(conf, Seconds(2))
    val textStream = ssc.textFileStream("file:///C:/Tmp/WebApiData")

    textStream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
