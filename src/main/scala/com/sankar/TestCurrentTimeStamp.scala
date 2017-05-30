package com.sankar

import org.apache.spark.sql.SparkSession

object TestCurrentTimeStamp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()

    import spark.sqlContext.implicits._

    spark.sql("select current_timestamp() as time, date_format(current_timestamp(),'W') as week").show
  }
}