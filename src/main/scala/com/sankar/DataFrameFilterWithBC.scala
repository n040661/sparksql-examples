package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameFilterWithBC {
  def main(args: Array[String]): Unit = {
    
		  val spark = SparkSession
				  .builder()
				  .appName("Spark SQL JSON example")
				  .master("local[*]")
				  .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
				  .getOrCreate()
				  val bc = spark.sparkContext.broadcast(Array[String]("login3", "login4"))
				  val x = Array(("login1", 192), ("login2", 146), ("login3", 72))
				  val xdf = spark.createDataFrame(x).toDF("name", "cnt")
				  
				  val func: (String, Int) => Boolean = (arg: String, number: Int) => bc.value.contains(arg)
				  val sqlfunc = udf(func)
				  val filtered = xdf.filter(sqlfunc(col("name"), col("cnt")))
				  
				  xdf.show()
				  filtered.show()
				  //val cols = array(col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"), col("name"))
  }
}