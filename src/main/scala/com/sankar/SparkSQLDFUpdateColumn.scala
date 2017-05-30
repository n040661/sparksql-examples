package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSQLDFUpdateColumn extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._

  /*val df = Seq(
						  (1, "dep1", "red"),(2, "dep1", "green"),(3, "dep1", "blue")
						  ).toDF("sno", "department", "status")
						  
						  df.show
						  
						  val df1 = df.where(col("status") === "red").select(col(""), col(""), col("status"))*/
  val df = Seq(
    (1, "fn", "red"),
    (2, "fn", "blue"),
    (3, "fn", "green"),
    (4, "aa", "blue"),
    (5, "aa", "green"),
    (6, "bb", "green"),
    (7, "bb", "yellow"),
    (8, "aa", "blue")).toDF("id", "fn", "color")

  //df.filter(df("color") === lit("red")).count

  val df1 = df.groupBy("fn")
    .agg(collect_set('color) as "values")
    .withColumn("hasRed", array_contains('values, "red"))
    .withColumn("hasBlue", array_contains('values, "blue"))
    
    df1.show()

  val result = df.join(df1, "fn")
    .withColumn("resultColor", when('hasRed, "red").when('hasBlue, "blue").otherwise("green"))
    //.withColumn("color", coalesce('resultColor, 'color))
    .withColumn("color", 'resultColor)
    .select('id, 'fn, 'color)
  result.show

  // val groupedDF = df.groupBy("fn").agg(countDistinct(col("color") === "red").as("count")).show()
}