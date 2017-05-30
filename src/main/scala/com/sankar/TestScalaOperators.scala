package com.sankar

object TestScalaOperators extends App {
  println(1 +: List(2, 3) :+ 4)
  println(1 +: List(2, 3))
  println(1 :: List(2, 3))
  println(List(2, 3).::(1))
  for (n <- 1 to 10) n % 2 match {
    case 0 => println("even")
    case 1 ⇒ println("odd")
  }
}