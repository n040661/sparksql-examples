package com.sankar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TestSparkSQLUnixTimeStamp {
  
  def main(args: Array[String]): Unit = {
    
		  val spark = SparkSession
				  .builder()
				  .appName("Spark SQL JSON example")
				  .master("local[*]")
				  .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
				  .getOrCreate()
				  
				  import spark.sqlContext.implicits._
				  
				  // Example data
				  val df = Seq((0,60),(61,120),(121,180)).toDF("minrange","maxrange")
				  
				  df.registerTempTable("range")
				  val intval = 20
					val rangeStr = spark.sqlContext.sql(s"select concat(MinRange, '-', MaxRange) from range where $intval>= MinRange and $intval < MaxRange").collectAsList().get(0).getString(0)
					println("THE RANGE IS**************:"+rangeStr)
						  
  }
}