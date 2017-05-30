package com.sankar

object TestScalaList extends App {
  val list = List("shankar","ramesh","aarush","bujji")
  println(list.toStream.filter { x => x.equals("ramesh") })
  println(list.filter { x => x.equals("bujji") })
  println(list.withFilter { x => x.equals("bujji") })
  val stream = 1 #:: 2 #:: 3 #:: Stream.empty
  stream.filter { x => x>1 }.foreach { x => println(x) }
  val stream1 = (1 to 100000000).toStream
  println(stream1.head)
  println(stream1.tail)
}