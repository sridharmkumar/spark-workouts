package com.spark.hive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object SparkHiveIntegration {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val hive = new HiveContext(sc)
    import hive.implicits._

    spark.sql("select * from txn.txnrecords_external").show()
  }

}
