package com.sankar

import org.apache.spark.sql.SparkSession

object TestScalaCompanionObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()
    val scalaClass = ScalaClassExample(spark)
    scalaClass.runSimpleSparkCode()
  }
}