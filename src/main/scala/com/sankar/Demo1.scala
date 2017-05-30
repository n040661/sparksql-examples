package com.sankar

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.SparkSession


object Demo1 {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
    .builder()
    .appName("Spark SQL CSV example")
    .master("local")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()
    val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    val numRuns = spark.sparkContext.accumulator(0) // to count the number of udf calls

    val myUdf = udf((i:Int) => {
      println("insideUDF******")
      numRuns.add(1)
      i.toDouble
      }
    )

    val data = spark.sparkContext.parallelize((1 to 2)).toDF("id")
    val data1 = data.withColumn("id", myUdf($"id"))
//    val data2 = data1.withColumn("id", $"id.a")

    val results = data1
//      .withColumn("id", myUdf($"id"))
//      .withColumn("id", $"id.a")
      .withColumn("c00", $"id")
      .withColumn("c01", $"id")
      .withColumn("c02", $"id")
      .withColumn("c03", $"id")
      .withColumn("c04", $"id")
      .withColumn("c05", $"id")
      .withColumn("c06", $"id")
      .withColumn("c07", $"id")
      .withColumn("c08", $"id")
      .withColumn("c09", $"id")
      .withColumn("c10", $"id")
      .withColumn("c11", $"id")
      .withColumn("c12", $"id")
      .withColumn("c13", $"id")
      .withColumn("c14", $"id")
      .withColumn("c15", $"id")
      .withColumn("c16", $"id")
      .withColumn("c17", $"id")
      .withColumn("c18", $"id")
      .withColumn("c19", $"id")
      .withColumn("c20", $"id")
      .withColumn("c21", $"id")
      .withColumn("c22", $"id")
      .withColumn("c23", $"id")
      .withColumn("c24", $"id")
      .withColumn("c25", $"id")
      .withColumn("c26", $"id")
      .withColumn("c27", $"id")
      .withColumn("c28", $"id")
      .withColumn("c29", $"id")
      .withColumn("c31", $"id")
      .withColumn("c32", $"id")
      .withColumn("c33", $"id")
      .withColumn("c34", $"id")
      .withColumn("c35", $"id")
      .withColumn("c36", $"id")
      .withColumn("c37", $"id")
      .withColumn("c38", $"id")
      .withColumn("c39", $"id")
      .withColumn("c40", $"id")
      .withColumn("c41", $"id")
      .withColumn("c42", $"id")


    val res = results.collect()
    println(res.size) // prints 100

    println(numRuns.value) // prints 160

  }
}