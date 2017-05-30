package com.sankar

object TestScalaMap extends App {
  var rtnMap = Map[Int, String]()
  var x = Map("AL" -> "Alabama")
  x += ("AK" -> "Alaska"); println(x)
  def getUserInputs {
    rtnMap += (1 -> "ss") //wrong here
  } 
  
}