package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.LeftOuter

object SparkSQLDFUnion extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._
  val df = Seq(
    ("STS", "red"),
    ("fn", "green"),
    ("aa", "blue")).toDF("channel", "color")

  val df1 = Seq(
    ("fn", "red"),
    ("aa", "amber")).toDF("channel", "color")

  val channelList = df1.select("channel").map(row => row.getString(0)).collectAsList()
  println(channelList)
  import scala.collection.JavaConversions._
  channelList.foreach { x => println(x) }
  channelList.foreach { x => println(x) }
  println(channelList.contains("fn"))

  //val resDF = df.unionAll(df1)
  //resDF.groupBy("channel").agg(collect_set("color") as "values")

  df.join(df1, df("channel") === df1("channel"), "leftouter")
    .where(!df("color").isin("green"))
    .select(df("channel"), df("color"))
    //    .explain
    .show

  df.join(df1, df("channel") === df1("channel"), "leftouter")
    .filter(not(df("color").isin("green")))
    .select(df("channel"), df("color"))
    //    .explain
    .show

}