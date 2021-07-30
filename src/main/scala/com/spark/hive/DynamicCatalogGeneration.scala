package com.spark.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DynamicCatalogGeneration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()

    val columnDataFrame = spark.read.format("csv").option("header", "true").load("src/main/resources/columns.csv")
    val listData = columnDataFrame.rdd.collect

    val prefix =
      """{
      "table":{"namespace":"default", "name":"hbase_tract10"},
      "rowkey":"masterid",
      "columns":{
      "masterid":{"cf":"rowkey", "col":"masterid", "type":"string"}, """ + System.lineSeparator()

    var middle = "" // second variable
    for (row <- listData) {
      var hiveColumn: String = "\"" + row.getString(1) + "\":{\"cf\":\"cf\"" + ", \"col\":\"" +
        row.getString(0) + "\"" + ", \"type\":\"string\"}," + System.lineSeparator()
      middle = middle + hiveColumn
    }
    middle = middle.trim().dropRight(1)
    val suffix = System.lineSeparator() + "}" + System.lineSeparator() + "}"

    val catalog = prefix + middle + suffix
    println(catalog)
  }
}
