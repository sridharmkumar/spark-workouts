package com.spark.revision

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkRevision {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark_integration").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("*** Revision Case #1 - Start ***")
    val intList = List(1, 4, 6, 7)
    intList.map(x => x + 1).foreach(println)
    println("*** Revision Case #1 - Complete ***")

    println
    println("*** Revision Case #2 - Start ***")
    val strList = List("Aathavan", "Aaruuran", "Abayan")
    strList.filter(x => x.contains("Aa")).foreach(println)
    println("*** Revision Case #2 - Complete ***")

    println
    println("*** Revision Case #3 - Start ***")
    val rawData = sc.textFile("src/main/resources/file1.txt")
    rawData.filter(x => x.contains("Gymnastics")).take(5).foreach(println)
    println("*** Revision Case #3 - Complete ***")

    println
    println("*** Revision Case #4 - Start ***")
    val mapData = rawData.map(x => x.split(","))
    val schemaRdd = mapData.map(x => Txns(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    schemaRdd.filter(x => x.product.contains("Gymnastics")).take(5).foreach(println)
    println("*** Revision Case #4 - Complete ***")

    println
    println("*** Revision Case #5 - Start ***")
    val fileData02 = sc.textFile("src/main/resources/file2.txt")
    val rowRdd = fileData02.map(x => x.split(",")).map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
    rowRdd.filter(x => x(8).toString.contains("cash")).take(5).foreach(println)
    println("*** Revision Case #5 - Complete ***")

    val columnList = List("category", "product", "txnno", "txndate", "amount", "city", "state", "spendby", "custno")

    println
    println("*** Revision Case #6 - Start ***")
    val schemaDataFrame = schemaRdd.toDF().select(columnList.map(col): _*)
    schemaDataFrame.show(5, truncate = false)

    val schemaStruct = StructType(Array(
      StructField("txnno", StringType, nullable = true),
      StructField("txndate", StringType, nullable = true),
      StructField("custno", StringType, nullable = true),
      StructField("amount", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("product", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("spendby", StringType, nullable = true)))

    val rowDataFrame = spark.createDataFrame(rowRdd, schemaStruct).select(columnList.map(col): _*)
    rowDataFrame.show(5)
    println("*** Revision Case #6 - Complete ***")

    println
    println("*** Revision Case #7 - Start ***")
    val csvDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("src/main/resources/file3.txt").select(columnList.map(col): _*)
    csvDataFrame.show(5, truncate = false)
    println("*** Revision Case #7 - Complete ***")

    println
    println("*** Revision Case #8 - Start ***")
    val jsonDataFrame = spark.read.format("json").load("src/main/resources/file4.json").select(columnList.map(col): _*)
    jsonDataFrame.show(5, truncate = false)

    val parquetDataFrame = spark.read.format("parquet").load("src/main/resources/file5.parquet").select(columnList.map(col): _*)
    parquetDataFrame.show(5, truncate = false)
    println("*** Revision Case #8 - Complete ***")

    println
    println("*** Revision Case #9 - Start ***")
    val xmlDataFrame = spark.read.format("xml").option("rowTag", "txndata").load("src/main/resources/file6").select(columnList.map(col): _*)
    xmlDataFrame.show(5, truncate = false)
    println("*** Revision Case #9 - Complete ***")

    println
    println("*** Revision Case #10 - Start ***")
    val unionDataFrames = schemaDataFrame.union(rowDataFrame).union(csvDataFrame).union(jsonDataFrame).union(parquetDataFrame).union(xmlDataFrame)
    unionDataFrames.show(20, truncate = false)
    println("*** Revision Case #10 - Complete ***")

    println
    println("*** Revision Case #11 - Start ***")
    val replaceWithTxnYear = unionDataFrames.withColumn("txnDate", expr("split(txnDate,'-')[2]"))
    val addStatusColumn = replaceWithTxnYear.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
    val finalData = addStatusColumn.filter(col("txnno") < 50000)
    finalData.show(20, truncate = false)
    println("*** Revision Case #11 - Complete ***")

    println
    println("*** Revision Case #12 - Start ***")
    finalData.write.format("com.databricks.spark.avro").partitionBy("category").mode("append").save("target/output/union_data")
    println("Final data written successfully")
    println("*** Revision Case #12 - Complete ***")
  }

  case class Txns(txnno: String, txndate: String, custno: String, amount: String, category: String, product: String, city: String, State: String, spendby: String)
}
