package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object ExplodeTest extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local")
    .config("spark.sql.warehouse.dir", "file:///C:\\Users\\sankar\\Desktop")
    .getOrCreate()
  val df = spark.read.json("C:\\Users\\sankar\\Desktop\\people.json")

  case class someModel(name: String, age: String)
  val explodedDF = df.explode(df("name"), df("age")) {
    case Row(nm: String, ae: String) => List(someModel(nm, ae))
  }
}