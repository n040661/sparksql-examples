package com.sankar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder

object SparkConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test Kafka").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(20))
    val sqlContext = new SQLContext(ssc.sparkContext)
    ssc.checkpoint("checkpoint")

    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet).map(_._2)

    messages.foreachRDD(rdd => {
      println("THE NUMBER OF RECORDS IN THE RDD IS:"+rdd.count())
      if (rdd.count() > 0) {
        //rdd.foreach { x => println(x) }
        //you can call apply all the rdd transformations and actions inside.
        //dont forget to call any action on RDD, otherwise it will not trigger the transformations.
      }
    })
    // Get the lines, split them into words, count the words and print
    //  val lines = messages.map(_._2)
    //  val words = lines.flatMap(_.split(" "))
    //  val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
    //  wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}