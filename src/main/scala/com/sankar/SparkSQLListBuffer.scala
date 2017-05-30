package com.sankar

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object SparkSQLListBuffer extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL JSON example")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
    .getOrCreate()

  val rowRDD = spark.sparkContext.parallelize(0 to 999).map(Row(_))
  val schema = StructType(StructField("value", IntegerType, true) :: Nil)
  val rowDF = spark.createDataFrame(rowRDD, schema)
  

  def hideTabooValues(taboo: Stream[Int]) = udf((n: Int) => if (taboo.contains(n)) -1 else n)

//  hideTabooValues _
  val forbiddenValues = List(0, 1, 2).toStream
  

  rowDF.select(hideTabooValues(forbiddenValues)(rowDF("value"))).show(6)
}