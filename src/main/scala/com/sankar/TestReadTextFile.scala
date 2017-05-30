package com.sankar

import org.apache.spark.sql.SparkSession

object TestReadTextFile extends App {
  
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate
    
    val  count = spark.sparkContext.parallelize(List(1, 2, 3, 4)).fold(1)(_ + _)
    println(count)
}