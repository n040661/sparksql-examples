package com.sankar

import org.apache.spark.sql.SparkSession
import java.util.regex.Pattern

object SparkMultiLineFormat {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL JSON example")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/IJava/")
      .getOrCreate()
    val wholeFile = spark.sparkContext.wholeTextFiles("C:\\Users\\sankar\\Desktop\\emp.txt")
    val departmentLog = wholeFile.map(
      tuple => {
        val p = Pattern.compile("\\r\\n[\\r\\n]+")
        p.split(tuple._2)
      })
    departmentLog.foreach { x =>
      {
        x.foreach {
          str =>
            {
              //println("THE RECORDS ARE*************" + x.size)
              //println(str)
              str.split("Project name").foreach { emp =>
                {
                  println(emp)
                  //                  val projectArray = emp.split("Project name|DOJ|DOL")
                  //                  println("The Project Name is "+projectArray(0))
                  //                  println("The DOJ is "+projectArray(1))
                  //                  println("The DOL is "+projectArray(2))
                  //println(x)
                }
              }
              //              str.split("Project name|DOJ|DOL").foreach {
              //                x => println(x)
              //              }
            }
        }
      }
    }
  }
}