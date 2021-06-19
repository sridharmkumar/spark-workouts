package com.spark.d0613

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}

object DifferentJoins {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val dataSet1 = spark.read.format("csv").option("header","true").load("file:///c://data//dataset1.csv")
    val dataSet2 = spark.read.format("csv").option("header","true").load("file:///c://data//dataset2.csv")

    println("***** Inner Join *****")
    val innerJoin = dataSet1.join(dataSet2, dataSet1("txnno")===dataSet2("txn_number"), "inner").drop(col("txn_number"))
    innerJoin.show(10, false)

    println("***** Outer Join *****")
    val outerJoin = dataSet1.join(dataSet2, dataSet1("txnno")===dataSet2("txn_number"), "outer").drop(col("txn_number"))
    outerJoin.show(10, false)

    println("***** Left Join *****")
    val leftJoin = dataSet1.join(dataSet2, dataSet1("txnno")===dataSet2("txn_number"), "left").drop(col("txn_number"))
    leftJoin.show(10, false)

    println("***** Right Join *****")
    val rightJoin = dataSet1.join(dataSet2, dataSet1("txnno")===dataSet2("txn_number"), "right").drop(col("txn_number"))
    rightJoin.show(10, false)
  }

}
