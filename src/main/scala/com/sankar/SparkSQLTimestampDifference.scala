package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.sql.Timestamp

object SparkSQLTimestampDifference extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._
  val df = Seq(
    ("STS", "red", "2016-12-11 20:40:10.234"),
    ("fn", "green", "2016-11-29 20:15:10.234"),
    ("aa", "blue", "2016-11-29 21:10:10.234")).toDF("channel", "color", "date")

  val diffFn = unix_timestamp($"date", "yyyy-MM-dd HH:mm:ss.S").cast("timestamp").between(Timestamp.valueOf(LocalDateTime.now().minusHours(1)), Timestamp.valueOf(LocalDateTime.now()))
  df.where(diffFn).show(false)
}