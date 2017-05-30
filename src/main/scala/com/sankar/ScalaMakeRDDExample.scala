package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object ScalaMakeRDDExample extends App {
  val conf = new SparkConf().setAppName("TestAPP")
  val sc = new SparkContext(conf)
  sc.makeRDD(Seq(1,2,3,4,5,6,7,8,9,10))
}