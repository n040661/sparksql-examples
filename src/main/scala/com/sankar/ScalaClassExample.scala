package com.sankar

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class ScalaClassExample(sparkContext: SparkContext) {
  
  def runSimpleSparkCode() {
    val rdd = sparkContext.parallelize(1 to 50, 2)
    rdd.foreach { x => println(x) }
  }
}

object ScalaClassExample {
  def apply(spark: SparkSession) = {
    new ScalaClassExample(spark.sparkContext)
  }
}