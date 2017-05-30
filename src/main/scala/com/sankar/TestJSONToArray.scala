package com.sankar

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestJSONToArray extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val df = spark.sparkContext.parallelize(Seq((1, "shankar", 1), (2, "ramesh", 2)), 2).toDF("id", "name", "score")
  
  df.write.json("C:\\TestData\\json")

}