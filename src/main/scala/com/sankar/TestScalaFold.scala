package com.sankar

object TestScalaFold {
  def main(args: Array[String]): Unit = {

    println(balance(List('a','b','c')))
  }

  def balance(chars: List[Char]): Boolean = chars.foldLeft(0) {
    case (0, ')') => return false
    case (x, ')') => x - 1
    case (x, '(') => x + 1
    case (x, _)   => x
  } == 0

}