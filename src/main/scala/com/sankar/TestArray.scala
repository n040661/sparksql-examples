package com.sankar

object TestArray extends App {
  val jul = List(1, 2, 3, 4, 5, 6)
  jul.dropWhile { x => x/2==0 }.foreach { x => println(x) }
  val sum = jul.foldLeft(0)(_ + _)
  println(sum)
  val partList = jul.partition { x => x%2==0 }
  println(partList)
}