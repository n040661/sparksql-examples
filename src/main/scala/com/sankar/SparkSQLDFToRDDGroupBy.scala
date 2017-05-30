package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._

object SparkSQLDFToRDDGroupBy extends App {

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
    (3, "", "green"),
    (4, "aa", "blue"),
    (5, "", "green"),
    (6, "bb", "red"),
    (7, "bb", "red"),
    (8, "aa", "blue")).toDF("id", "fn", "color")

  df.show()

  val rdd = df.rdd
  val filteredRDD = rdd.filter { row =>
    {
      val fn = row.getString(1)
      println("THE FUNCTION VALUE IS:"+fn)
      val color = row.getString(2)
      if(color.equalsIgnoreCase("green")) true
      else false
    }
  }

  println(filteredRDD.count())
  filteredRDD.foreach { x => println(x) }
  filteredRDD.saveAsTextFile("C:\\Users\\sankar\\Desktop\\frdd")

}