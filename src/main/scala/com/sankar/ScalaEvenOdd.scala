package com.sankar

object ScalaEvenOdd extends App {
  def isAllEven(n: Int) = s"$n".forall(_.asDigit % 2 == 0)
  println(isAllEven(2246))
}