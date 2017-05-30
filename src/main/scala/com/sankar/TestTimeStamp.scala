package com.sankar

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.Duration
import java.time.temporal.ChronoUnit

object TestTimeStamp {
  def main(args: Array[String]): Unit = {
    //def twice(f: Int => Int) = f compose f
    def twice(f: (Int => Int))(i: Int) = f(i)
    //println(twice(_ + 3)(7))
    println(twice(_ *3)( 3))
  }
}