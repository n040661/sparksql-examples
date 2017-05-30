package com.sankar

import scala.io.Source

object TestReadFile {
  def main(args: Array[String]): Unit = {
    val empMap = scala.collection.mutable.Map[Tuple2[String, String], String]().empty
    for(line <- Source.fromFile("C://Users//sankar//Desktop//workingdays.txt").getLines().drop(1)) {
      val lineArr = line.split(",")
      empMap += (lineArr(0) -> "Sunday" -> lineArr(1))
      empMap += (lineArr(0) -> "Monday" -> lineArr(2))
      empMap += (lineArr(0) -> "Tuesday" -> lineArr(3))
      empMap += (lineArr(0) -> "Wednesday" -> lineArr(4))
      empMap += (lineArr(0) -> "Thursday" -> lineArr(5))
      empMap += (lineArr(0) -> "Friday" -> lineArr(6))
      empMap += (lineArr(0) -> "Saturday" -> lineArr(7))
    }
    //empMap.foreach(println)
    println(empMap.get(("SG"->"Tuesday")).get)
  }
}