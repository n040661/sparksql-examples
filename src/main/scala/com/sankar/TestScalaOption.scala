package com.sankar

object TestScalaOption {
  def main(args: Array[String]): Unit = {

  }
  def isBlank(input: Option[String]): Boolean =
    {
      val str = input.getOrElse("")
      input.isEmpty || input.filter(_.trim.length > 0).isEmpty
    }
}