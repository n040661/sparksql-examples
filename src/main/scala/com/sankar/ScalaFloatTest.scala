package com.sankar

object ScalaFloatTest extends App {
  sealed trait List[+A]
  case object Nil extends List[Nothing]
  case class Cons[+A](head: A, tail: List[A]) extends List[A]
  //  val nums: List[Int] = List[Int](1, 2, 3)
  val numsList: List[Int] = Cons(1, Cons(2, Cons(3, Nil)))
  println(numsList)
  println(List("a", "b", "c") zip (Stream from 10))
}