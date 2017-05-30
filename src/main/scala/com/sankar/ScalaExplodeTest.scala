package com.sankar

import org.apache.spark.sql.SparkSession

class ScalaExplodeTest {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  val employeeData = List(("Jack",1000.0),("Bob",2000.0),("Carl",7000.0))
  val seqExam = Seq(1,2,3,4,5,6,7,8,9,10)
 //val employeeRDD = spark.sparkContext.makeRDD(employeeData)
  val employeeRDD = spark.sparkContext.makeRDD(seqExam)
  employeeRDD.foreach { x => println(x) }
}