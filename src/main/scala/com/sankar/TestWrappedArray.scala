package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray

object TestWrappedArray {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()

    import spark.sqlContext.implicits._
    val df = Seq(
      (1, "fn", "red"),
      (2, "fn", "blue"),
      (3, "fn", "green"),
      (4, "aa", "blue"),
      (5, "aa", "green"),
      (6, "bb", "green"),
      (7, "bb", "yellow"),
      (8, "aa", "blue")).toDF("id", "fn", "color")

    val df1 = df.groupBy("fn")
      .agg(collect_set('color) as "values")

    val resMap = df1.map { x => (x(0).toString(),x(1).asInstanceOf[WrappedArray[String]]) }.collect().toMap
    val colorList = resMap.get("aa").get.toSeq
    if(colorList.contains("blue")) {
      println("BLUE IS THERE IN THE LIST")
    }
    if(colorList.contains("yellow")) {
      println("YELLOW IS THERE IN THE LIST")
    } else {
      println("YELLOW IS NOT THERE IN THE LIST")
    }
  }
}