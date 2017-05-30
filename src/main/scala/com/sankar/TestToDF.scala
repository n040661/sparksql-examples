package com.sankar

import org.apache.spark.sql.SparkSession

object TestToDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()

    import spark.implicits._
    case class Test(a: String, b: String, c: String, d: String)

  }
}