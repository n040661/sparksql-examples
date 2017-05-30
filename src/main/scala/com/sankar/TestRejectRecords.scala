package com.sankar

import org.apache.spark.sql.SparkSession

object TestRejectRecords {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()

    val rdd = spark.sparkContext.textFile("C:\\Users\\sankar\\Desktop\\test.txt")
    val filteredRDD = rdd.filter { record =>
      {
        val cols = record.split("\",\"")
        val txnUID = cols(0)
        val date = cols(1)
        val name = cols(2)
        if(txnUID.isEmpty()) {
          println("INSIE TXN CHECK")
          true
        }
        else if(date.isEmpty()) {
          println("INSIE DATE CHECK")
          true
        }
        else if(name.isEmpty()) {
          println("INSIE NAME CHECK")
          true
        }
        else {
          println("DEFAULT")
          false
        }
      }
    }
    println(filteredRDD.count)
    filteredRDD.foreach { x => println(x) }
    filteredRDD.saveAsTextFile("C:\\Users\\sankar\\Desktop\\reject")
  }
}