package com.sankar

object ScalaPartialFunction {
  def main(args: Array[String]): Unit = {

    println(divider(0))
  }
  val divider : PartialFunction[Int,Int] = {
    case d : Int if d != 0 => 42/d
    case _ => 0
  }
}