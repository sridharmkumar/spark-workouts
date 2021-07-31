package com.spark.hbase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DynamicHBaseHiveIntegration {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val hive = new HiveContext(sc)

    val columnData = spark.read.format("csv").option("header", "true").load("file:///home/cloudera/data/columns.csv")
    println()
    println("****** Column Mapping Details ******")
    println()
    columnData.show(false)

    val columnDataAsList = columnData.rdd.collect
    val prefix =
      """{
      "table":{"namespace":"default", "name":"hbase_tract10"},
      "rowkey":"masterid",
      "columns":{
      "masterid":{"cf":"rowkey", "col":"masterid", "type":"string"}, """ + System.lineSeparator()

    var middle = "" // second variable
    for (row <- columnDataAsList) {
      var hiveColumn: String = "\"" + row.getString(1) + "\":{\"cf\":\"cf\"" + ", \"col\":\"" +
        row.getString(0) + "\"" + ", \"type\":\"string\"}," + System.lineSeparator()
      middle = middle + hiveColumn
    }
    middle = middle.trim().dropRight(1)
    val suffix = System.lineSeparator() + "}" + System.lineSeparator() + "}"
    val catalog = prefix + middle + suffix
    println()
    println("****** Generated HBase Catalog ******")
    println(catalog)

    println()
    println("****** Reading Data from HBase ******")
    println()
    val hbaseData = spark.read.options(Map(HBaseTableCatalog.tableCatalog -> catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()

    println("HBase Schema:")
    hbaseData.printSchema()

    println()
    println("HBase Records:")
    hbaseData.show(5, truncate = false)

    println()
    println("****** Writing Data into Distinct Table Names ******")
    println()
    val derivedTableNames = columnDataAsList.map(x => x.getString(1).split("_")(0)).distinct
    for (derivedVal <- derivedTableNames) {
      val columnNames = hbaseData.columns.filter(x => x.contains("masterid") || x.startsWith(derivedVal))
      val finalTableName = "project_4." + derivedVal.concat("_tab")
      println("Writing Data for table - " + finalTableName)

      val finalData = hbaseData.select(columnNames.head, columnNames.tail: _*)
      println("Schema:")
      finalData.printSchema()

      finalData.write.mode("overwrite").format("hive").saveAsTable(finalTableName)
      println()
    }
    println()
    println("****** Data Written Successfully! ******")
    println()
  }
}