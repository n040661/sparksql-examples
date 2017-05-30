package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DFUserFilter extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._
  
  val df = Seq( "ramesh", "tony", "Tony", "tony1", "san tony").toDF("user")
  df.select("*").filter(col("user").rlike("^tony$")).show()
}
