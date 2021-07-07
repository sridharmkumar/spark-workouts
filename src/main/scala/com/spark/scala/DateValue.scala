package com.spark.scala

object DateValue {
  def main(args: Array[String]): Unit = {
    val dateValue = java.time.LocalDate.now().minusDays(1)
    println("Yesterday Date is " + dateValue)

  }

}
