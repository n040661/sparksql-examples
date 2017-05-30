package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object TestReadWriteJSON extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  import spark.sqlContext.implicits._
  val df = spark.read.json("src/main/resources/people.json")
  df.show()
  //df.distinct().show
  df.dropDuplicates(Seq("name")).show
  /*val w = Window.partitionBy($"age").orderBy($"name".desc)
  val modifiedDF = df.withColumn("rn", row_number().over(w))//.where($"rn" <= 3).drop("rn")
  modifiedDF.show()*/
//  df.limit(1).show()
//  df.map(row => row.mkString("$")).show()
//  df.select(concat($"name", lit(" "), $"age")).show()
//  df.withColumn("newColumnName", concat($"name", lit(" "), $"age")).show()
//  val array = df.select("age").rdd.map(r => r(0).asInstanceOf[Long]).collect()
 // array.foreach { x => println(x) }
  
  
}