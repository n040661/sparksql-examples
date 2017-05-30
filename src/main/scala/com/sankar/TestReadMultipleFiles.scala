package com.sankar

import org.apache.spark.sql.SparkSession

object TestReadMultipleFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()
    //    val pairRDD = spark.sparkContext.wholeTextFiles("C:\\Users\\sankar\\Desktop\\textfiles\\*")
    //    val rdd = pairRDD.map(tuple => tuple._2)
    //    println(rdd.count())
    //    rdd.foreach { x => println(x) }

    val rdd = spark.sparkContext.textFile("C:\\Users\\sankar\\Desktop\\textfiles\\*\\*")
    println(rdd.count())
    rdd.foreach { x => println(x) }
  }
}